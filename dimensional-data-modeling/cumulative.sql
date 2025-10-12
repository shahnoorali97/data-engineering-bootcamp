-- Cumulative table generation query:
WITH recent_year AS (
    SELECT actorid, MAX(year) AS latest_year
    FROM actor_films
    WHERE year <= :target_year
    GROUP BY actorid
),
avg_ratings AS (
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
    WHERE af.year <= :target_year
    GROUP BY af.actorid, ry.latest_year
),
active_status AS (
    SELECT
        actorid,
        (MAX(CASE WHEN year = :target_year THEN 1 ELSE 0 END) > 0) AS is_active
    FROM actor_films
    WHERE year <= :target_year
    GROUP BY actorid
)
INSERT INTO actors (actor_name, actor_id, films, quality_class, is_active)
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
WHERE af.year <= :target_year
GROUP BY af.actor, af.actorid, ar.quality_class, ac.is_active
ON CONFLICT (actor_id)
DO UPDATE SET
    films = EXCLUDED.films,             -- replace or merge film JSON
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active
    