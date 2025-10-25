SELECT
    user_id,
    device_id,
    device_activity_datelist,
    ARRAY(
        SELECT CAST(TO_CHAR(d, 'YYYYMMDD') AS INT)
        FROM unnest(device_activity_datelist) AS d
        ORDER BY d
    ) AS datelist_int
FROM user_devices_cumulated

ALTER TABLE user_devices_cumulated
ADD COLUMN datelist_int INT[]

UPDATE user_devices_cumulated
SET datelist_int = ARRAY(
    SELECT CAST(TO_CHAR(d, 'YYYYMMDD') AS INT)
    FROM unnest(device_activity_datelist) AS d
    ORDER BY d
)
