CREATE TABLE sessions_by_ip_host (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    num_events BIGINT
)

-- What is the average number of web events of a session from a user on Tech Creator?

SELECT 
    ROUND(AVG(num_events), 2) AS avg_events_per_session
FROM sessions_by_ip_host
WHERE host LIKE '%.techcreator.io'

-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT 
    host,
    COUNT(*) AS total_sessions,
    ROUND(AVG(num_events), 2) AS avg_events_per_session,
    MAX(num_events) AS max_events_in_session,
    MIN(num_events) AS min_events_in_session
FROM sessions_by_ip_host
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC

