CREATE TABLE host_activity_reduced (
    month_start DATE NOT NULL,                 -- first day of the month
    host TEXT NOT NULL,                        -- domain or hostname
    hit_array INT[] NOT NULL,                  -- daily hits for the month
    unique_visitors_array INT[] NOT NULL,      -- daily unique visitors for the month
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_start, host)
)

WITH daily_agg AS (
    SELECT
        host,
        DATE(event_time::timestamp) AS event_date,
        COUNT(*) AS hits,
        COUNT(DISTINCT user_id) AS unique_visitors
    FROM events
    WHERE host IS NOT NULL
      AND event_time::timestamp >= DATE '2023-01-01'   -- replace with the day you’re loading
      AND event_time::timestamp <  DATE '2023-01-02'   -- next day
    GROUP BY host, DATE(event_time::timestamp)
),
agg_with_month AS (
    SELECT
        host,
        DATE_TRUNC('month', event_date)::DATE AS month_start,
        ARRAY[hits] AS hit_array,
        ARRAY[unique_visitors] AS unique_visitors_array
    FROM daily_agg
)
INSERT INTO host_activity_reduced (month_start, host, hit_array, unique_visitors_array)
SELECT
    a.month_start,
    a.host,
    CASE
        WHEN h.hit_array IS NOT NULL THEN h.hit_array || a.hit_array
        ELSE a.hit_array
    END AS hit_array,
    CASE
        WHEN h.unique_visitors_array IS NOT NULL THEN h.unique_visitors_array || a.unique_visitors_array
        ELSE a.unique_visitors_array
    END AS unique_visitors_array
FROM agg_with_month a
LEFT JOIN host_activity_reduced h
    ON a.month_start = h.month_start
   AND a.host = h.host
ON CONFLICT (month_start, host)
DO UPDATE
SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array,
    updated_at = CURRENT_TIMESTAMP
