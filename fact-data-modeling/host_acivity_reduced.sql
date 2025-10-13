CREATE TABLE host_activity_reduced (
    month_start DATE NOT NULL,                 -- first day of the month
    host TEXT NOT NULL,                        -- domain or hostname
    hit_array INT[] NOT NULL,                  -- daily hits for the month
    unique_visitors_array INT[] NOT NULL,      -- daily unique visitors for the month
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_start, host)
);
