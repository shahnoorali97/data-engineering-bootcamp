CREATE TABLE actors_history_scd (
    actor_history_id SERIAL PRIMARY KEY,   -- unique surrogate key for SCD record
    actor_id TEXT NOT NULL,                -- business key
    actor_name TEXT NOT NULL,              -- actor name for readability
    quality_class TEXT NOT NULL,           -- 'star', 'good', 'average', 'bad'
    is_active BOOLEAN NOT NULL,            -- whether actor was active in that period
    start_date DATE NOT NULL,              -- when this version started
    end_date DATE,                         -- when it ended (NULL = current)
    is_current BOOLEAN DEFAULT TRUE,       -- marks current active record
    CONSTRAINT uq_actor_version UNIQUE (actor_id, start_date) -- ensures no overlapping versions
)

INSERT INTO actors_history_scd (actor_id, actor_name, quality_class, is_active, start_date)
SELECT
    a.actor_id,
    a.actor_name,
    a.quality_class,
    a.is_active,
    CURRENT_DATE
FROM actors a
LEFT JOIN actors_history_scd h
  ON a.actor_id = h.actor_id
  AND h.is_current = TRUE
WHERE
  h.actor_id IS NULL  -- new actor
  OR a.quality_class <> h.quality_class
  OR a.is_active <> h.is_active

-- STEP 1: Close old versions that have changed
WITH changed_actors AS (
    SELECT h.actor_history_id, a.actor_id
    FROM actors a
    JOIN actors_history_scd h
      ON a.actor_id = h.actor_id
     AND h.is_current = TRUE
    WHERE a.quality_class <> h.quality_class
       OR a.is_active <> h.is_active
)
UPDATE actors_history_scd
SET end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
WHERE actor_history_id IN (SELECT actor_history_id FROM changed_actors)

-- STEP 2: Insert new versions for changed or new actors
INSERT INTO actors_history_scd (
    actor_id,
    actor_name,
    quality_class,
    is_active,
    start_date,
    end_date,
    is_current
)
SELECT
    a.actor_id,
    a.actor_name,
    a.quality_class,
    a.is_active,
    CURRENT_DATE AS start_date,
    NULL AS end_date,
    TRUE AS is_current
FROM actors a
LEFT JOIN actors_history_scd h
  ON a.actor_id = h.actor_id
 AND h.is_current = TRUE
WHERE h.actor_id IS NULL  -- new actor
   OR a.quality_class <> h.quality_class
   OR a.is_active <> h.is_active
   
SELECT
    a.actor_id,
    a.actor_name AS current_actor_name,
    a.quality_class AS current_quality,
    a.is_active AS current_is_active,
    h.quality_class AS scd_quality,
    h.is_active AS scd_is_active,
    h.start_date,
    h.end_date,
    h.is_current
FROM actors a
LEFT JOIN actors_history_scd h
  ON a.actor_id = h.actor_id
 AND h.is_current = TRUE
ORDER BY a.actor_id