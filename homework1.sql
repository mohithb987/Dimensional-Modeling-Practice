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
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);

-- 2. Cumulative table generation query: Write a query that populates the actors table one year at a time
SELECT * FROM actors;
INSERT INTO actors
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
    w.films,
    w.avg_rating,
    w.quality_class,
    (w.films[cardinality(w.films)]::films).year = w.year AS is_active,
    w.year
FROM windowed w;


-- SELECT * from windowed ORDER BY actor, year;

SELECT * FROM actors;

-- 3. DDL for actors_history_scd table:

DROP TABLE actors_scd;
CREATE TABLE actors_scd
(
    actor         TEXT,
    actorid       TEXT,
    quality_class quality_class,
    is_active     BOOLEAN,
    start_year    INTEGER,
    end_year      INTEGER,
    current_year  INTEGER,
    PRIMARY KEY   (actorid, start_year)
);




INSERT INTO actors_scd
WITH with_previous AS
    (SELECT actor,
            actorid,
            quality_class,
            is_active,
            LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_quality_class,
            LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_is_active,
            current_year
     FROM actors
     WHERE current_year <= 2018
   ),
    previous_with_change_indicator AS
    (SELECT *,
            CASE WHEN (quality_class <> previous_quality_class OR is_active <> previous_is_active)
                    THEN 1
                    ELSE 0
            END as change_indicator
     FROM with_previous
   ),
    previous_with_streak_identifier AS
    (SELECT *,
            SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY current_year) as streak_identifier
     FROM previous_with_change_indicator)

    SELECT
        actor,
        actorid,
        quality_class,
        is_active,
        MIN(current_year) as start_year,
        MAX(current_year) as end_year,
        2018 as current_year
    FROM previous_with_streak_identifier
    GROUP BY actor, actorid, streak_identifier, is_active, quality_class   --streak_identifier is IMP to group by changes
    ORDER BY actorid, start_year;

SELECT * FROM actors_scd ORDER BY actor, start_year;
SELECT COUNT(*) FROM actors_scd;
-- incrementally update SCD table from 2019.
CREATE TYPE actors_scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_year INTEGER,
    end_year INTEGER
);

CREATE OR REPLACE FUNCTION process_actors_by_year()
    RETURNS VOID AS $$
BEGIN
    FOR input_year IN 2019..2022 LOOP
INSERT INTO actors_scd
WITH last_year_scd AS(                -- these are the records of concern, as they either ended last year, or would continue further.
    SELECT * from actors_scd
             WHERE current_year=input_year-1
             AND end_year=input_year-1
),
    historical_scd AS (               -- records that we aren't concerned about in the incremental load
    SELECT
        actor,
        actorid,
        quality_class,
        is_active,
        start_year,
        end_year
    FROM actors_scd
    WHERE current_year=input_year-1 AND end_year<input_year-1
    ),
    this_year_data AS (
    SELECT * from actors
         WHERE current_year=input_year
    ),
    --increment SCD end season with current season for unchanged records.
    unchanged_records AS (
        SELECT
            ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ls.start_year,
            ts.current_year AS end_season,
            input_year as current_year
            FROM this_year_data ts
            JOIN last_year_scd ls
            ON ls.actorid=ts.actorid
            WHERE ts.quality_class = ls.quality_class
            AND ts.is_active=ls.is_active
    ),
    changed_records AS (
        SELECT
            ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ts.current_year as start_year,
            ts.current_year as end_year,
            input_year as current_year
            FROM this_year_data ts
            LEFT JOIN last_year_scd ls
            ON ls.actorid=ts.actorid
        WHERE (ts.quality_class <> ls.quality_class OR ts.is_active<>ls.is_active)
    ),
    new_records AS (
        SELECT
            ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ts.current_year as start_year,
            ts.current_year as end_year,
            input_year as current_year
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ts.actorid=ls.actorid
        WHERE ls.actorid IS NULL
    )
    SELECT *
    FROM unchanged_records
    UNION ALL
    SELECT *
    FROM changed_records
    UNION ALL
    SELECT *
    from new_records
    ON CONFLICT (actorid, start_year)
    DO UPDATE
    SET end_year = EXCLUDED.end_year, current_year=input_year
;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT process_actors_by_year();

SELECT * from actors_scd ORDER BY actor, start_year;
