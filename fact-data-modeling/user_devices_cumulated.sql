CREATE TABLE user_devices_cumulated (
    user_id TEXT NOT NULL,
    browser_type TEXT NOT NULL,
    device_activity_datelist DATE[],
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, browser_type)
)