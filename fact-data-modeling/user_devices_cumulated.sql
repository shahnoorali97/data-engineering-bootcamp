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
  GROUP BY user_id, device_id, DATE(event_time)
)