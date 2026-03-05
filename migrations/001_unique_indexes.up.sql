-- Unique index for dsp_requests: name=1 and name=2 use (name, gid) with oid,duration NULL
CREATE UNIQUE INDEX IF NOT EXISTS idx_dsp_requests_name_gid
ON dsp_requests (name, gid)
WHERE oid IS NULL AND duration IS NULL;

-- Unique index for dsp_requests: name=3 uses (name, gid, oid, duration)
CREATE UNIQUE INDEX IF NOT EXISTS idx_dsp_requests_name_gid_oid_duration
ON dsp_requests (name, gid, oid, duration)
WHERE oid IS NOT NULL;

-- Unique index for dsp_statistics: one row per (dsp_request_id, date, hour)
CREATE UNIQUE INDEX IF NOT EXISTS idx_dsp_statistics_request_date_hour
ON dsp_statistics (dsp_request_id, date, hour);
