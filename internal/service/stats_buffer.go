package service

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"dsp-statistics-go/internal/db"
)

type statsKey struct {
	DspRequestID int
	Date         time.Time
	Hour         int
}

type statsAggregate struct {
	MinBid    float64
	MaxBid    float64
	SumBid    float64
	Count     int
	Histogram map[string]int
}

// StatsBuffer holds in-memory aggregates per (dsp_request_id, date, hour) and flushes to DB periodically.
type StatsBuffer struct {
	mu   sync.Mutex
	data map[statsKey]*statsAggregate
}

func formatBid(bid float64) string {
	return strconv.FormatFloat(bid, 'f', -1, 64)
}

var (
	defaultBuffer     *StatsBuffer
	defaultBufferOnce sync.Once
)

func getDefaultBuffer() *StatsBuffer {
	defaultBufferOnce.Do(func() {
		defaultBuffer = &StatsBuffer{data: make(map[statsKey]*statsAggregate)}
	})
	return defaultBuffer
}

// Add accumulates one bid into the buffer (thread-safe).
func (b *StatsBuffer) Add(dspRequestID int, date time.Time, hour int, minPrice float64) {
	if minPrice == 0.0 {
		minPrice = 0.01
	}
	priceKey := formatBid(minPrice)
	k := statsKey{DspRequestID: dspRequestID, Date: date, Hour: hour}
	b.mu.Lock()
	agg, ok := b.data[k]
	if !ok {
		agg = &statsAggregate{
			MinBid:    minPrice,
			MaxBid:    minPrice,
			SumBid:    minPrice,
			Count:     1,
			Histogram: map[string]int{priceKey: 1},
		}
		b.data[k] = agg
	} else {
		if minPrice < agg.MinBid {
			agg.MinBid = minPrice
		}
		if minPrice > agg.MaxBid {
			agg.MaxBid = minPrice
		}
		agg.SumBid += minPrice
		agg.Count++
		if agg.Histogram == nil {
			agg.Histogram = make(map[string]int)
		}
		agg.Histogram[priceKey]++
	}
	b.mu.Unlock()
}

// Flush writes all buffered aggregates to the DB and clears the buffer (thread-safe).
func (b *StatsBuffer) Flush() error {
	b.mu.Lock()
	snapshot := b.data
	b.data = make(map[statsKey]*statsAggregate)
	b.mu.Unlock()

	if len(snapshot) == 0 {
		return nil
	}

	ctx := context.Background()
	pool := db.GetDB()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		// Restore snapshot on failure so we don't lose data
		b.mu.Lock()
		for k, v := range snapshot {
			if cur, ok := b.data[k]; ok {
				cur.SumBid += v.SumBid
				cur.Count += v.Count
				if v.MinBid < cur.MinBid {
					cur.MinBid = v.MinBid
				}
				if v.MaxBid > cur.MaxBid {
					cur.MaxBid = v.MaxBid
				}
			} else {
				b.data[k] = v
			}
		}
		b.mu.Unlock()
		return err
	}
	defer conn.Release()

	for k, agg := range snapshot {
		avgBid := agg.SumBid / float64(agg.Count)
		_, err := conn.Exec(ctx, `
			INSERT INTO dsp_statistics (dsp_request_id, date, hour, min_bid, max_bid, avg_bid, bid_count, bid_histogram, closed, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, '{}'::jsonb), false, NOW(), NOW())
			ON CONFLICT (dsp_request_id, date, hour) DO UPDATE SET
				min_bid = LEAST(dsp_statistics.min_bid, EXCLUDED.min_bid),
				max_bid = GREATEST(dsp_statistics.max_bid, EXCLUDED.max_bid),
				bid_count = dsp_statistics.bid_count + EXCLUDED.bid_count,
				avg_bid = (dsp_statistics.avg_bid * dsp_statistics.bid_count + EXCLUDED.avg_bid * EXCLUDED.bid_count) / (dsp_statistics.bid_count + EXCLUDED.bid_count),
				bid_histogram = (
					SELECT jsonb_object_agg(key, value_sum)
					FROM (
						SELECT key, SUM(value_int) AS value_sum
						FROM (
							SELECT key, ((value::text)::int) AS value_int
							FROM jsonb_each(COALESCE(dsp_statistics.bid_histogram, '{}'::jsonb))
							UNION ALL
							SELECT key, ((value::text)::int) AS value_int
							FROM jsonb_each(COALESCE(EXCLUDED.bid_histogram, '{}'::jsonb))
						) combined
						GROUP BY key
					) s
				),
				updated_at = NOW()`,
			k.DspRequestID, k.Date, k.Hour, agg.MinBid, agg.MaxBid, avgBid, agg.Count, agg.Histogram)
		if err != nil {
			// Restore this and remaining entries
			b.mu.Lock()
			if cur, ok := b.data[k]; ok {
				cur.SumBid += agg.SumBid
				cur.Count += agg.Count
				if agg.MinBid < cur.MinBid {
					cur.MinBid = agg.MinBid
				}
				if agg.MaxBid > cur.MaxBid {
					cur.MaxBid = agg.MaxBid
				}
			} else {
				b.data[k] = agg
			}
			for k2, agg2 := range snapshot {
				if k2 != k {
					if cur, ok := b.data[k2]; ok {
						cur.SumBid += agg2.SumBid
						cur.Count += agg2.Count
						if agg2.MinBid < cur.MinBid {
							cur.MinBid = agg2.MinBid
						}
						if agg2.MaxBid > cur.MaxBid {
							cur.MaxBid = agg2.MaxBid
						}
					} else {
						b.data[k2] = agg2
					}
				}
			}
			b.mu.Unlock()
			log.Printf("StatsBuffer flush error: %v", err)
			return err
		}
	}
	return nil
}

// RunStatsBufferFlusher starts a goroutine that flushes the default buffer periodically and on ctx.Done().
// Call from consumer main with the same context so shutdown triggers a final flush.
func RunStatsBufferFlusher(ctx context.Context) {
	intervalSec := 10
	if v := os.Getenv("STATS_FLUSH_INTERVAL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			intervalSec = n
		}
	}
	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()
	buf := getDefaultBuffer()
	for {
		select {
		case <-ctx.Done():
			if err := buf.Flush(); err != nil {
				log.Printf("StatsBuffer final flush error: %v", err)
			}
			return
		case <-ticker.C:
			if err := buf.Flush(); err != nil {
				log.Printf("StatsBuffer flush error: %v", err)
			}
		}
	}
}
