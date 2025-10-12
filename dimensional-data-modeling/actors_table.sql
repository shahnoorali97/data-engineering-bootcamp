-- DDL for actors table:

CREATE TABLE actors (
    actor_name TEXT,
    actor_id TEXT PRIMARY KEY,
    films JSONB,          -- stores an array of film objects
    quality_class TEXT,   -- 'star', 'good', 'average', or 'bad'
    is_active BOOLEAN     -- whether the actor is active this year
)

INSERT INTO actors (actor_name, actor_id, films, quality_class, is_active)
WITH recent_year AS (
    -- find each actor's most recent film year
    SELECT actorid, MAX(year) AS latest_year
    FROM actor_films
    GROUP BY actorid
),
avg_ratings AS (
    -- calculate average rating in that recent year
    SELECT
        af.actorid,
        CASE
            WHEN AVG(af.rating) FILTER (WHERE af.year = ry.latest_year) > 8 THEN 'star'
            WHEN AVG(af.rating) FILTER (WHERE af.year = ry.latest_year) > 7 THEN 'good'
            WHEN AVG(af.rating) FILTER (WHERE af.year = ry.latest_year) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class
    FROM actor_films af
    JOIN recent_year ry ON af.actorid = ry.actorid
    GROUP BY af.actorid, ry.latest_year
),
active_status AS (
    -- determine whether the actor is active this year
    SELECT
        actorid,
        (MAX(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE) THEN 1 ELSE 0 END) > 0) AS is_active
    FROM actor_films
    GROUP BY actorid
)
SELECT
    af.actor AS actor_name,
    af.actorid AS actor_id,
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'film', af.film,
            'votes', af.votes,
            'rating', af.rating,
            'filmid', af.filmid
        ) ORDER BY af.year DESC
    ) AS films,
    ar.quality_class,
    ac.is_active
FROM actor_films af
JOIN avg_ratings ar ON af.actorid = ar.actorid
JOIN active_status ac ON af.actorid = ac.actorid
GROUP BY af.actor, af.actorid, ar.quality_class, ac.is_active

SELECT * FROM actors

SELECT
    actor_name,
    jsonb_array_elements(films)->>'film' AS film_title,
    jsonb_array_elements(films)->>'rating' AS film_rating
FROM actors
WHERE quality_class = 'star'