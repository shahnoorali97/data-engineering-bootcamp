CREATE TABLE user_devices_cumulated (
    user_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    device_activity_datelist DATE[],
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, device_id)
);

WITH daily_activity AS (
    SELECT
        user_id,
        device_id,
        DATE(event_time) AS activity_date
    FROM events
    WHERE user_id IS NOT NULL
      AND device_id IS NOT NULL   -- filter out nulls
    GROUP BY user_id, device_id, DATE(event_time)
),

agg_daily AS (
    SELECT
        user_id,
        device_id,
        ARRAY_AGG(DISTINCT activity_date ORDER BY activity_date) AS new_dates
    FROM daily_activity
    GROUP BY user_id, device_id
)
INSERT INTO user_devices_cumulated (user_id, device_id, device_activity_datelist)
SELECT
    a.user_id,
    a.device_id,
    CASE
        WHEN u.device_activity_datelist IS NOT NULL THEN
            ARRAY(
                SELECT DISTINCT unnest(u.device_activity_datelist || a.new_dates)
                ORDER BY 1
            )
        ELSE a.new_dates
    END AS device_activity_datelist
FROM agg_daily a
LEFT JOIN user_devices_cumulated u
    ON a.user_id::TEXT = u.user_id
   AND a.device_id::TEXT = u.device_id
ON CONFLICT (user_id, device_id)
DO UPDATE
SET
    device_activity_datelist = EXCLUDED.device_activity_datelist,
    updated_at = CURRENT_TIMESTAMP
