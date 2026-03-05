package service

import (
	"context"
	"log"
	"time"

	"dsp-statistics-go/internal/core"
	"dsp-statistics-go/internal/db"
)

var DSP_NAME = map[string]int{
	"Dron":        2,
	"CityScreen":  3,
	"RussOutdoor": 1,
}

func ProcessPayload(payload core.Payload) error {
	name, exists := DSP_NAME[payload.Name]
	if !exists {
		return nil // Name not found, skip processing
	}

	dspRequest, err := FindOrCreateDspRequest(name, payload)
	if err != nil {
		return err
	}

	if dspRequest == nil {
		log.Printf("DSP request not created for payload: %+v", payload)
		return nil
	}

	showTime := time.Unix(payload.ShowTimeTs, 0).Truncate(time.Hour)
	hour := showTime.Hour()
	minPrice := payload.MinPrice
	getDefaultBuffer().Add(dspRequest.ID, showTime, hour, minPrice)
	return nil
}

// FindOrCreateDspRequest uses INSERT ... ON CONFLICT (upsert) for a single DB round-trip.
func FindOrCreateDspRequest(name int, payload core.Payload) (*core.DspRequest, error) {
	ctx := context.Background()
	conn := db.GetDB()

	var dspRequest core.DspRequest

	switch name {
	case 1:
		if payload.CodeFromOwner == nil {
			return nil, nil
		}
		gid := string(*payload.CodeFromOwner)
		err := conn.QueryRow(ctx, `
			INSERT INTO dsp_requests (name, gid, created_at, updated_at)
			VALUES ($1, $2, NOW(), NOW())
			ON CONFLICT (name, gid) WHERE (oid IS NULL AND duration IS NULL)
			DO UPDATE SET updated_at = NOW()
			RETURNING id, name, gid, oid, duration`,
			name, gid).Scan(&dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)
		if err != nil {
			return nil, err
		}
		dspRequest.GID = gid
		return &dspRequest, nil

	case 2:
		if payload.GID == nil {
			return nil, nil
		}
		gid := *payload.GID
		err := conn.QueryRow(ctx, `
			INSERT INTO dsp_requests (name, gid, created_at, updated_at)
			VALUES ($1, $2, NOW(), NOW())
			ON CONFLICT (name, gid) WHERE (oid IS NULL AND duration IS NULL)
			DO UPDATE SET updated_at = NOW()
			RETURNING id, name, gid, oid, duration`,
			name, gid).Scan(&dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)
		if err != nil {
			return nil, err
		}
		dspRequest.GID = gid
		return &dspRequest, nil

	case 3:
		if payload.GID == nil || payload.OID == nil || payload.Duration == nil {
			return nil, nil
		}
		err := conn.QueryRow(ctx, `
			INSERT INTO dsp_requests (name, gid, oid, duration, created_at, updated_at)
			VALUES ($1, $2, $3, $4, NOW(), NOW())
			ON CONFLICT (name, gid, oid, duration) WHERE (oid IS NOT NULL)
			DO UPDATE SET updated_at = NOW()
			RETURNING id, name, gid, oid, duration`,
			name, *payload.GID, *payload.OID, *payload.Duration).Scan(
			&dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)
		if err != nil {
			return nil, err
		}
		return &dspRequest, nil

	default:
		return nil, nil
	}
}

// UpdateStatistics uses a single INSERT ... ON CONFLICT (upsert) instead of SELECT + INSERT/UPDATE.
func UpdateStatistics(dspRequestID int, minPrice float64, showTimeTs int64) error {
	ctx := context.Background()
	conn := db.GetDB()

	newMinBid := minPrice
	if newMinBid == 0.0 {
		newMinBid = 0.01
	}

	showTime := time.Unix(showTimeTs, 0).Truncate(time.Hour)
	hour := showTime.Hour()

	_, err := conn.Exec(ctx, `
		INSERT INTO dsp_statistics (dsp_request_id, date, hour, min_bid, max_bid, avg_bid, bid_count, closed, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $4, $4, 1, false, NOW(), NOW())
		ON CONFLICT (dsp_request_id, date, hour) DO UPDATE SET
			min_bid = LEAST(dsp_statistics.min_bid, EXCLUDED.min_bid),
			max_bid = GREATEST(dsp_statistics.max_bid, EXCLUDED.max_bid),
			bid_count = dsp_statistics.bid_count + EXCLUDED.bid_count,
			avg_bid = (dsp_statistics.avg_bid * dsp_statistics.bid_count + EXCLUDED.min_bid * EXCLUDED.bid_count) / (dsp_statistics.bid_count + EXCLUDED.bid_count),
			updated_at = NOW()`,
		dspRequestID, showTime, hour, newMinBid)
	return err
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
