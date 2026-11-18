-- PC3 CC531 — Resultados de las operaciones policiales a nivel nacional [MININTER]
-- Esquema en estrella: 2 tablas dimension + 1 tabla de hechos.

DROP TABLE IF EXISTS hecho_resultado_operativo;
DROP TABLE IF EXISTS dim_variable;
DROP TABLE IF EXISTS dim_dependencia;

CREATE TABLE dim_dependencia (
    dependencia_id   SERIAL PRIMARY KEY,
    nombre           VARCHAR(100) NOT NULL UNIQUE,
    dir_reg_fre      VARCHAR(30)  NOT NULL
);

CREATE TABLE dim_variable (
    variable_id      SERIAL PRIMARY KEY,
    nombre           VARCHAR(100) NOT NULL UNIQUE,
    categoria        VARCHAR(40)  NOT NULL,
    unidad           VARCHAR(20)  NOT NULL
);

CREATE TABLE hecho_resultado_operativo (
    id               SERIAL PRIMARY KEY,
    anio             INTEGER      NOT NULL,
    mes              VARCHAR(3)   NOT NULL,
    dependencia_id   INTEGER      NOT NULL REFERENCES dim_dependencia(dependencia_id),
    variable_id      INTEGER      NOT NULL REFERENCES dim_variable(variable_id),
    valor            NUMERIC(14,3) NOT NULL
);

CREATE INDEX idx_hecho_anio ON hecho_resultado_operativo(anio);
CREATE INDEX idx_hecho_dependencia ON hecho_resultado_operativo(dependencia_id);
CREATE INDEX idx_hecho_variable ON hecho_resultado_operativo(variable_id);
