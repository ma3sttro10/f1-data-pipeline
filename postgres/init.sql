-- =============================================================
-- Schéma en étoile pour l'analytique F1
-- =============================================================

-- ── Dimensions ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id       VARCHAR(10)  PRIMARY KEY,
    driver_name     VARCHAR(100) NOT NULL,
    nationality     VARCHAR(50),
    current_team    VARCHAR(100),
    date_of_birth   DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_circuit (
    circuit_id      SERIAL       PRIMARY KEY,
    year            SMALLINT     NOT NULL,
    round           SMALLINT     NOT NULL,
    gp_name         VARCHAR(100) NOT NULL,
    circuit_name    VARCHAR(100),
    country         VARCHAR(50),
    city            VARCHAR(50),
    lat             DECIMAL(9,6),
    lng             DECIMAL(9,6),
    UNIQUE (year, round)  -- Empêche les doublons lors des chargements Spark
);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id         SERIAL       PRIMARY KEY,
    team_name       VARCHAR(100) UNIQUE NOT NULL,
    nationality     VARCHAR(50),
    constructor_id  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_tyre (
    tyre_id         SERIAL      PRIMARY KEY,
    compound        VARCHAR(20) UNIQUE NOT NULL, -- SOFT, MEDIUM, HARD, WET, INTER
    colour_hex      VARCHAR(7)
);

-- Seed des pneus
INSERT INTO dim_tyre (compound, colour_hex) VALUES
    ('SOFT',       '#FF0000'),
    ('MEDIUM',     '#FFFF00'),
    ('HARD',       '#FFFFFF'),
    ('INTERMEDIATE','#00FF00'),
    ('WET',        '#0000FF')
ON CONFLICT DO NOTHING;

-- ── Table de faits principale ─────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_race_result (
    result_id       BIGSERIAL    PRIMARY KEY,
    year            SMALLINT     NOT NULL,
    round           SMALLINT     NOT NULL,
    gp_name         VARCHAR(100) NOT NULL,
    driver_id       VARCHAR(10)  REFERENCES dim_driver(driver_id),
    team            VARCHAR(100),
    position        SMALLINT,
    points          DECIMAL(4,1),
    status          VARCHAR(50),
    is_winner       BOOLEAN      DEFAULT FALSE,
    is_points_finish BOOLEAN     DEFAULT FALSE,
    ingested_at     TIMESTAMP    DEFAULT NOW(),
    UNIQUE (year, round, driver_id)  -- Idempotence Spark
);

-- ── Pit stops ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_pit_stop (
    pit_id          BIGSERIAL   PRIMARY KEY,
    year            SMALLINT    NOT NULL,
    round           SMALLINT    NOT NULL,
    gp_name         VARCHAR(100),
    driver_id       VARCHAR(10) REFERENCES dim_driver(driver_id),
    lap_number      SMALLINT    NOT NULL,
    pit_duration_sec DECIMAL(6,3),
    compound        VARCHAR(20),
    tyre_life       SMALLINT,
    ingested_at     TIMESTAMP   DEFAULT NOW(),
    UNIQUE (year, round, driver_id, lap_number)
);

-- ── Météo ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id      BIGSERIAL   PRIMARY KEY,
    year            SMALLINT    NOT NULL,
    gp_name         VARCHAR(100) NOT NULL,
    air_temp_c      DECIMAL(4,1),
    track_temp_c    DECIMAL(4,1),
    humidity_pct    DECIMAL(4,1),
    wind_speed_ms   DECIMAL(4,1),
    rainfall        BOOLEAN     DEFAULT FALSE,
    ingested_at     TIMESTAMP   DEFAULT NOW()
);

-- ── Index analytiques ─────────────────────────────────────────
-- Ces index accélèrent les requêtes du dashboard Streamlit

CREATE INDEX IF NOT EXISTS idx_race_year      ON fact_race_result(year);
CREATE INDEX IF NOT EXISTS idx_race_driver    ON fact_race_result(driver_id);
CREATE INDEX IF NOT EXISTS idx_race_team      ON fact_race_result(team);
CREATE INDEX IF NOT EXISTS idx_pit_year_round ON fact_pit_stop(year, round);
CREATE INDEX IF NOT EXISTS idx_pit_driver     ON fact_pit_stop(driver_id);

-- ── Vues analytiques (utilisées par Streamlit) ────────────────

CREATE OR REPLACE VIEW vw_driver_standings AS
SELECT
    year,
    driver_id,
    MAX(driver_name_denorm.driver_name) AS driver_name,  -- dénormalisé pour perf
    SUM(points)                         AS total_points,
    COUNT(*) FILTER (WHERE is_winner)   AS wins,
    COUNT(*) FILTER (WHERE is_points_finish) AS points_finishes,
    RANK() OVER (PARTITION BY year ORDER BY SUM(points) DESC) AS championship_rank
FROM fact_race_result frr
-- Jointure avec une sous-requête pour éviter le produit cartésien
LEFT JOIN (
    SELECT driver_id, driver_name FROM dim_driver
) AS driver_name_denorm USING (driver_id)
GROUP BY year, driver_id;

CREATE OR REPLACE VIEW vw_team_standings AS
SELECT
    year,
    team,
    SUM(points)  AS total_points,
    COUNT(*) FILTER (WHERE is_winner) AS wins,
    RANK() OVER (PARTITION BY year ORDER BY SUM(points) DESC) AS championship_rank
FROM fact_race_result
GROUP BY year, team;

CREATE OR REPLACE VIEW vw_pit_stop_strategy AS
SELECT
    p.year,
    p.gp_name,
    p.driver_id,
    COUNT(*)              AS total_stops,
    AVG(pit_duration_sec) AS avg_pit_duration_sec,
    MIN(pit_duration_sec) AS fastest_pit_sec,
    STRING_AGG(compound, ' → ' ORDER BY lap_number) AS tyre_strategy
FROM fact_pit_stop p
GROUP BY p.year, p.gp_name, p.driver_id;