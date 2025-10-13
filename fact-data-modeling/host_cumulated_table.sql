CREATE TABLE hosts_cumulated (
    host TEXT NOT NULL,                        -- the domain or hostname
    host_activity_datelist DATE[] NOT NULL,    -- list of dates when this host had activity
    datelist_int INT[],                        -- optional: same dates in YYYYMMDD integer format
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (host)
)