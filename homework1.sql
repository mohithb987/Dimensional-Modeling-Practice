-- SELECT * FROM actor_films;

-- 1. DDL for actors table:
DROP TYPE IF EXISTS quality_class;
CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

DROP TYPE IF EXISTS films CASCADE;
CREATE TYPE films AS (
    year INTEGER,
    film TEXT,
    filmid TEXT,
    votes INTEGER,
    rating REAL
);
DROP TABLE IF EXISTS actors;
CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    avg_rating REAL,
    quality_class TEXT,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);

-- 2. Cumulative table generation query: Write a query that populates the actors table one year at a time

WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1970, 2022) AS year
),
actors_first_year AS (
    SELECT
        actor,
        actorid,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actor, actorid
),
actors_and_years AS (
    SELECT *
    FROM actors_first_year
    JOIN years y
        ON actors_first_year.first_year <= y.year
),
windowed AS (
    SELECT DISTINCT
        aay.actor,
        aay.actorid,
        aay.year,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN af.year IS NOT NULL
                        THEN ROW(
                            af.year,
                            af.film,
                            af.filmid,
                            af.votes,
                            af.rating
                        )::films
                END)
            OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)),
            NULL
        ) AS films,
        AVG(af.rating::NUMERIC) OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)) AS avg_rating,
        CASE
            WHEN AVG(af.rating::NUMERIC) OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)) > 8 THEN 'star'
            WHEN AVG(af.rating::NUMERIC) OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)) > 7 AND AVG(af.rating::NUMERIC) OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)) <= 8 THEN 'good'
            WHEN AVG(af.rating::NUMERIC) OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)) > 6 AND AVG(af.rating::NUMERIC) OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year, af.year)) <= 7 THEN 'average'
            ELSE 'bad'
        END::quality_class AS quality_class
    FROM actors_and_years aay
    LEFT JOIN actor_films af
        ON aay.actor = af.actor
        AND aay.year = af.year
    ORDER BY aay.actor, aay.year
)
SELECT
    w.actor,
    w.actorid,
    w.year,
    w.films,
    w.avg_rating,
    w.quality_class,
    (w.films[cardinality(w.films)]::films).year = w.year AS is_active
FROM windowed w;


-- SELECT * from windowed ORDER BY actor, year;
