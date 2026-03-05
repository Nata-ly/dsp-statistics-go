# Database migrations

Run the up migration before using the optimized consumer so that unique indexes exist for upserts.

**Up (apply):**
```bash
psql "$DATABASE_URL" -f migrations/001_unique_indexes.up.sql
```

**Down (rollback):**
```bash
psql "$DATABASE_URL" -f migrations/001_unique_indexes.down.sql
```

Required indexes:
- `dsp_requests`: partial unique on `(name, gid)` where `oid IS NULL AND duration IS NULL`; partial unique on `(name, gid, oid, duration)` where `oid IS NOT NULL`.
- `dsp_statistics`: unique on `(dsp_request_id, date, hour)`.
