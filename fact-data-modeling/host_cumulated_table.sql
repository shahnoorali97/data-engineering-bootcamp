CREATE TABLE hosts_cumulated (
    host TEXT NOT NULL,                        -- the domain or hostname
    host_activity_datelist DATE[] NOT NULL,    -- list of dates when this host had activity
    datelist_int INT[],                        -- optional: same dates in YYYYMMDD integer format
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (host)
)

WITH daily_activity AS (
    SELECT
        host,
        DATE(event_time) AS activity_date
    FROM events
    WHERE host IS NOT NULL
    GROUP BY host, DATE(event_time)
),
agg_daily AS (
    SELECT
        host,
        ARRAY_AGG(DISTINCT activity_date ORDER BY activity_date) AS new_dates
    FROM daily_activity
    GROUP BY host
)
INSERT INTO hosts_cumulated (host, host_activity_datelist)
SELECT
    a.host,
    CASE
        WHEN h.host_activity_datelist IS NOT NULL THEN
            ARRAY(
                SELECT DISTINCT unnest(h.host_activity_datelist || a.new_dates)
                ORDER BY 1
            )
        ELSE a.new_dates
    END AS host_activity_datelist
FROM agg_daily a
LEFT JOIN hosts_cumulated h
    ON a.host = h.host
ON CONFLICT (host)
DO UPDATE
SET
    host_activity_datelist = EXCLUDED.host_activity_datelist,
    updated_at = CURRENT_TIMESTAMP

