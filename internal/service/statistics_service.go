package service

import (
    "context"
    "log"
    "time"

    "dsp-statistics-go/internal/core"
    "dsp-statistics-go/internal/db"

    "github.com/jackc/pgx/v4"
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

    return UpdateStatistics(dspRequest.ID, payload.MinPrice, payload.ShowTimeTs)
}

func FindOrCreateDspRequest(name int, payload core.Payload) (*core.DspRequest, error) {
    ctx := context.Background()
    conn := db.GetDB()

    var dspRequest core.DspRequest
    var err error

    switch name {
    case 1:
        if payload.CodeFromOwner == nil {
            return nil, nil
        }

        // Сначала пытаемся найти существующую запись
        err = conn.QueryRow(ctx, `
            SELECT id, name, gid, oid, duration
            FROM dsp_requests
            WHERE name = $1 AND gid = $2`,
            name, string(*payload.CodeFromOwner)).Scan(
            &dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)

        if err != nil {
            if err == pgx.ErrNoRows {
                // Если запись не найдена, создаем новую
                _, err = conn.Exec(ctx, `
                    INSERT INTO dsp_requests (name, gid)
                    VALUES ($1, $2)`,
                    name, string(*payload.CodeFromOwner))
                if err != nil {
                    return nil, err
                }

                // Повторно ищем запись
                err = conn.QueryRow(ctx, `
                    SELECT id, name, gid, oid, duration
                    FROM dsp_requests
                    WHERE name = $1 AND gid = $2`,
                    name, string(*payload.CodeFromOwner)).Scan(
                    &dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)
            }
            if err != nil {
                return nil, err
            }
        }

    case 2:
        if payload.GID == nil {
            return nil, nil
        }

        // Сначала пытаемся найти существующую запись
        err = conn.QueryRow(ctx, `
            SELECT id, name, gid, oid, duration
            FROM dsp_requests
            WHERE name = $1 AND gid = $2`,
            name, *payload.GID).Scan(
            &dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)

        if err != nil {
            if err == pgx.ErrNoRows {
                // Если запись не найдена, создаем новую
                _, err = conn.Exec(ctx, `
                    INSERT INTO dsp_requests (name, gid)
                    VALUES ($1, $2)`,
                    name, *payload.GID)
                if err != nil {
                    return nil, err
                }

                // Повторно ищем запись
                err = conn.QueryRow(ctx, `
                    SELECT id, name, gid, oid, duration
                    FROM dsp_requests
                    WHERE name = $1 AND gid = $2`,
                    name, *payload.GID).Scan(
                    &dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)
            }
            if err != nil {
                return nil, err
            }
        }

    case 3:
        if payload.GID == nil || payload.OID == nil || payload.Duration == nil {
            return nil, nil
        }

        // Сначала пытаемся найти существующую запись
        err = conn.QueryRow(ctx, `
            SELECT id, name, gid, oid, duration
            FROM dsp_requests
            WHERE name = $1 AND gid = $2 AND oid = $3 AND duration = $4`,
            name, *payload.GID, *payload.OID, *payload.Duration).Scan(
            &dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)

        if err != nil {
            if err == pgx.ErrNoRows {
                // Если запись не найдена, создаем новую
                _, err = conn.Exec(ctx, `
                    INSERT INTO dsp_requests (name, gid, oid, duration)
                    VALUES ($1, $2, $3, $4)`,
                    name, *payload.GID, *payload.OID, *payload.Duration)
                if err != nil {
                    return nil, err
                }

                // Повторно ищем запись
                err = conn.QueryRow(ctx, `
                    SELECT id, name, gid, oid, duration
                    FROM dsp_requests
                    WHERE name = $1 AND gid = $2 AND oid = $3 AND duration = $4`,
                    name, *payload.GID, *payload.OID, *payload.Duration).Scan(
                    &dspRequest.ID, &dspRequest.Name, &dspRequest.GID, &dspRequest.OID, &dspRequest.Duration)
            }
            if err != nil {
                return nil, err
            }
        }
    default:
        return nil, nil
    }

    if err != nil {
        return nil, err
    }

    return &dspRequest, nil
}

func UpdateStatistics(dspRequestID int, minPrice float64, showTimeTs int64) error {
    ctx := context.Background()
    conn := db.GetDB()

    newMinBid := minPrice
    if newMinBid == 0.0 {
        newMinBid = 0.01
    }

    showTime := time.Unix(showTimeTs, 0).Truncate(time.Hour)
    hour := showTime.Hour()

    var stats struct {
        ID       int
        MinBid   float64
        AvgBid   float64
        BidCount int
    }

    err := conn.QueryRow(ctx, `
        SELECT id, min_bid, avg_bid, bid_count
        FROM dsp_statistics
        WHERE dsp_request_id = $1 AND date = $2 AND hour = $3`,
        dspRequestID, showTime, hour).Scan(&stats.ID, &stats.MinBid, &stats.AvgBid, &stats.BidCount)

    if err != nil {
        if err == pgx.ErrNoRows {
            // Create new stats record
            _, err = conn.Exec(ctx, `
                INSERT INTO dsp_statistics (dsp_request_id, date, hour, min_bid, avg_bid, bid_count, closed)
                VALUES ($1, $2, $3, $4, $4, 1, false)`,
                dspRequestID, showTime, hour, newMinBid)
            return err
        }
        return err
    }

    // Update existing stats
    newAvgBid := (stats.AvgBid*float64(stats.BidCount) + newMinBid) / float64(stats.BidCount+1)
    newMinBid = minFloat(stats.MinBid, newMinBid)

    _, err = conn.Exec(ctx, `
        UPDATE dsp_statistics
        SET min_bid = $1, avg_bid = $2, bid_count = bid_count + 1
        WHERE id = $3`,
        newMinBid, newAvgBid, stats.ID)

    return err
}

func minFloat(a, b float64) float64 {
    if a < b {
        return a
    }
    return b
}
