-- Step 1: Merge duplicate dsp_statistics (same dsp_request_id, date, hour): keep min(id), aggregate stats, delete others
WITH grouped AS (
  SELECT dsp_request_id, date, hour,
    MIN(id) AS keep_id,
    MIN(min_bid) AS new_min, MAX(max_bid) AS new_max,
    (SUM(avg_bid * bid_count) / NULLIF(SUM(bid_count), 0)) AS new_avg,
    SUM(bid_count) AS new_count
  FROM dsp_statistics
  GROUP BY dsp_request_id, date, hour
  HAVING COUNT(*) > 1
)
UPDATE dsp_statistics s
SET min_bid = g.new_min, max_bid = g.new_max, avg_bid = g.new_avg, bid_count = g.new_count
FROM grouped g
WHERE s.id = g.keep_id;

DELETE FROM dsp_statistics a
USING dsp_statistics b
WHERE a.dsp_request_id = b.dsp_request_id AND a.date = b.date AND a.hour = b.hour AND a.id > b.id;

-- Step 2: Merge duplicate dsp_requests (name, gid) where oid IS NULL: point stats to kept id, delete others
WITH dups AS (
  SELECT name, gid, MIN(id) AS keep_id
  FROM dsp_requests
  WHERE oid IS NULL AND duration IS NULL
  GROUP BY name, gid
  HAVING COUNT(*) > 1
)
UPDATE dsp_statistics s
SET dsp_request_id = d.keep_id
FROM dsp_requests r
JOIN dups d ON d.name = r.name AND d.gid = r.gid
WHERE s.dsp_request_id = r.id AND r.id != d.keep_id;

DELETE FROM dsp_requests r
USING (
  SELECT name, gid, MIN(id) AS keep_id
  FROM dsp_requests
  WHERE oid IS NULL AND duration IS NULL
  GROUP BY name, gid
  HAVING COUNT(*) > 1
) d
WHERE r.name = d.name AND r.gid = d.gid AND r.oid IS NULL AND r.duration IS NULL AND r.id != d.keep_id;

-- Step 3: Merge duplicate dsp_requests (name, gid, oid, duration) where oid IS NOT NULL
WITH dups3 AS (
  SELECT name, gid, oid, duration, MIN(id) AS keep_id
  FROM dsp_requests
  WHERE oid IS NOT NULL
  GROUP BY name, gid, oid, duration
  HAVING COUNT(*) > 1
)
UPDATE dsp_statistics s
SET dsp_request_id = d.keep_id
FROM dsp_requests r
JOIN dups3 d ON d.name = r.name AND d.gid = r.gid AND d.oid = r.oid AND d.duration = r.duration
WHERE s.dsp_request_id = r.id AND r.id != d.keep_id;

DELETE FROM dsp_requests r
USING (
  SELECT name, gid, oid, duration, MIN(id) AS keep_id
  FROM dsp_requests
  WHERE oid IS NOT NULL
  GROUP BY name, gid, oid, duration
  HAVING COUNT(*) > 1
) d
WHERE r.name = d.name AND r.gid = d.gid AND r.oid = d.oid AND r.duration = d.duration AND r.id != d.keep_id;

-- Step 4: Create unique indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_dsp_requests_name_gid
ON dsp_requests (name, gid)
WHERE oid IS NULL AND duration IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dsp_requests_name_gid_oid_duration
ON dsp_requests (name, gid, oid, duration)
WHERE oid IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dsp_statistics_request_date_hour
ON dsp_statistics (dsp_request_id, date, hour);
