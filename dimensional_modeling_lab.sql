SELECT * FROM player_seasons;

CREATE TYPE season_stats AS
(
    season INTEGER, gp INTEGER, pts REAL, reb REAL, ast REAL
);

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

DROP TABLE players;
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);


SELECT min(season) from player_seasons;
SELECT * from players;

INSERT INTO players
WITH yesterday AS (
    SELECT * FROM players
             WHERE current_season = (SELECT MAX(current_season) FROM players)
),
    today AS (
    SELECT * FROM player_seasons
             WHERE season = COALESCE((SELECT 1+MAX(current_season) from players), (SELECT MIN(season) from player_seasons))
)
SELECT
    COALESCE(t.player_name, y.player_name) as player_name,
    COALESCE(t.height, y.height) as height,
    COALESCE(t.college, y.college) as college,
    COALESCE(t.country, y.country) as country,
    COALESCE(t.draft_year, y.draft_year) as draft_year,
    COALESCE(t.draft_round, y.draft_round) as draft_round,
    COALESCE(t.draft_number, y.draft_number) as draft_number,
    CASE WHEN y.season_stats IS NULL
            THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
         WHEN t.season IS NOT NULL
            THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
         ELSE
            y.season_stats
    END as season_stats,
    CASE WHEN t.season IS NOT NULL THEN
        CASE WHEN t.pts>20 THEN 'star'
             WHEN t.pts>15 THEN 'good'
             WHEN t.pts>10 THEN 'average'
             ELSE 'bad'
        END::scoring_class
    ELSE y.scoring_class
    END,
    CASE WHEN t.season IS NOT NULL THEN 0
         ELSE COALESCE(y.years_since_last_season, 0) + 1
    END as years_since_last_season,
    CASE WHEN COALESCE(y.years_since_last_season, 0)=0 THEN true ELSE false END as is_active,
    COALESCE(t.season, y.current_season + 1) as current_season
FROM today t
    FULL OUTER JOIN yesterday y
        ON t.player_name = y.player_name;

SELECT * from players;

WITH unnested AS (
    SELECT player_name, unnest(season_stats)::season_stats AS season_stats
    from players
)
SELECT player_name, (season_stats::season_stats).*
FROM unnested;

SELECT player_name, ((season_stats[1])::season_stats).pts as first_season_pts,
       ((season_stats[cardinality(season_stats)])::season_stats).pts as last_season_pts,
       CASE WHEN ((season_stats[1])::season_stats).pts=0
            THEN 0
            ELSE ((season_stats[cardinality(season_stats)])::season_stats).pts/((season_stats[1])::season_stats).pts
       END as diff
FROM players
WHERE current_season = 2001
ORDER BY diff DESC
LIMIT 5;

ALTER TABLE players ADD COLUMN is_active BOOLEAN;
UPDATE players SET is_active=true WHERE years_since_last_season=0;
UPDATE players SET is_active=false WHERE years_since_last_season!=0;


-- CONVERTING TO SCD TYPE 2

SELECT player_name, current_season, scoring_class, is_active
FROM players
where current_season=2022;

DROP TABLE IF EXISTS players_scd;
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, start_season)
);

-- backfill SCD table from earliest season to 2021.

INSERT INTO players_scd
WITH with_previous AS
    (SELECT player_name,
            scoring_class,
            is_active,
            LAG(scoring_class, 1)
            OVER (PARTITION BY player_name ORDER BY current_season)                   as previous_scoring_class,
            LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active,
            current_season
     FROM players
     WHERE current_season <= 2021
   ),
    with_indicator AS
    (SELECT *,
            CASE WHEN (scoring_class <> previous_scoring_class OR is_active <> previous_is_active)
                    THEN 1
                    ELSE 0
            END as change_indicator
     FROM with_previous
   ),
    with_streaks AS
    (SELECT *,
            SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) as streak_identifier
     FROM with_indicator)

    SELECT
        player_name,
        scoring_class,
        is_active,
        MIN(current_season) as start_season,
        MAX(current_season) as end_season,
        2021 as current_season
    FROM with_streaks
    GROUP BY player_name, streak_identifier, is_active, scoring_class
    ORDER BY player_name, start_season
;

select  * from players_scd;


-- incrementally update SCD table from 2021.
CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active boolean,
    start_season INTEGER,
    end_season INTEGER
);

WITH last_season_scd AS(
    SELECT * from players_scd
             WHERE current_season=2021
             AND end_season=2021
),
    historical_scd AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season=2021
    AND end_season<2021
    ),
    this_season_data AS (
    SELECT * from players
         WHERE current_season=2022
    ),
    --increment SCD end season with current season for unchanged records.
    unchanged_records AS (
        SELECT ts.player_name, ts.scoring_class, ls.scoring_class, ts.is_active, ls.start_season, ts.current_season AS end_season
            FROM this_season_data ts
            JOIN last_season_scd ls
            ON ls.player_name=ts.player_name
        WHERE ts.scoring_class = ls.scoring_class
        AND ts.is_active=ls.is_active
    ),
    changed_records AS (
        SELECT
            ts.player_name,
            UNNEST(
                    ARRAY[ROW(
                                ls.scoring_class,
                                ls.is_active,
                                ls.start_season,
                                ls.end_season
                                )::scd_type,
                                ROW(
                                ts.scoring_class,
                                ts.is_active,
                                ts.current_season,
                                ts.current_season
                                )::scd_type
                    ]
            ) as unnested
            FROM this_season_data ts
            LEFT JOIN last_season_scd ls
            ON ls.player_name=ts.player_name
        WHERE (ts.scoring_class <> ls.scoring_class OR ts.is_active=ls.is_active)
    ),
    unnested_changed_records AS (
        SELECT player_name, (unnested::scd_type).*
        FROM changed_records
        ),
    new_records AS (
        SELECT
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ts.current_season as start_season,
            ts.current_season as end_season
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ts.player_name=ls.player_name
        WHERE ls.player_name IS NULL
    )
    SELECT * from historical_scd
    UNION ALL SELECT * from unnested_changed_records
    UNION ALL SELECT * from new_records
    ORDER BY player_name, start_season
;
