/*******************************************************************************************
 * Créatiop d''une bdd de centralisation des données STOC depuis les portails VisioNature  *
 *******************************************************************************************/

CREATE SCHEMA IF NOT EXISTS pr_stoc AUTHORIZATION geonature;

CREATE TABLE IF NOT EXISTS pr_stoc.l_grille (
    id        INTEGER NOT NULL PRIMARY KEY,
    area      FLOAT,
    perimeter FLOAT,
    carrenat  INTEGER,
    numnat    INTEGER,
    x_coord   FLOAT,
    y_coord   FLOAT,
    geom      GEOMETRY(Polygon, 2154)
);

ALTER TABLE pr_stoc.l_grille
    OWNER TO geonature;

CREATE INDEX l_grille_geom_idx
    ON pr_stoc.l_grille USING gist(geom);


ALTER TABLE pr_stoc.l_grille
    ADD CONSTRAINT l_grille_numnat UNIQUE (numnat)
;


/* Table des points */
-- drop table if exists pr_stoc.t_releves cascade;
CREATE TABLE pr_stoc.t_releves (
    id                  SERIAL NOT NULL PRIMARY KEY,
    date                DATE,
    heure               TIME,
    observateur         VARCHAR(100),
    carre_numnat        INTEGER,
    point_num           INTEGER,
    altitude            INTEGER,
    nuage               INTEGER,
    pluie               INTEGER,
    vent                INTEGER,
    visibilite          INTEGER,
    p_milieu            VARCHAR(10),
    p_type              VARCHAR(10),
    p_cat1              VARCHAR(10),
    p_cat2              VARCHAR(10),
    p_ss_cat1           VARCHAR(10),
    p_ss_cat2           VARCHAR(10),
    s_milieu            VARCHAR(10),
    s_type              VARCHAR(10),
    s_cat1              VARCHAR(10),
    s_cat2              VARCHAR(10),
    s_ss_cat1           VARCHAR(10),
    s_ss_cat2           VARCHAR(10),
    site                BOOLEAN,
    passage_mnhn        VARCHAR(10),
    source_bdd          VARCHAR(50),
    source_id           TEXT[],
    source_id_universal VARCHAR(50) UNIQUE,
    type_eps            VARCHAR(20),
    geom                GEOMETRY(point, 2154),
    CONSTRAINT type_esp_con CHECK (type_eps IN ('Point', 'Transect') OR type_eps IS NULL)
);


/* Index sur les colonnes carre_numnat, date et point_num */

CREATE UNIQUE INDEX ON pr_stoc.t_releves(source_id_universal);
CREATE UNIQUE INDEX ON pr_stoc.t_releves(carre_numnat, date, point_num, source_id_universal);
CREATE INDEX ON pr_stoc.t_releves(carre_numnat, date, point_num);

CREATE INDEX ON pr_stoc.t_releves
    USING gist(geom)
;


/* Table des observations */
-- drop table if exists pr_stoc.t_observations cascade
-- ;

CREATE TABLE pr_stoc.t_observations (
    id            SERIAL NOT NULL PRIMARY KEY,
    carre_numnat  INTEGER,
    date          DATE,
    time          TIME,
    id_releve     INTEGER REFERENCES pr_stoc.t_releves(id)
        ON DELETE CASCADE,
    point         INTEGER,
    codesp_euring VARCHAR(10),
    nombre        INTEGER,
    source        VARCHAR(20),
    passage       VARCHAR(20),
    distance_v2   VARCHAR(50),
    id_source     TEXT[]
)
;

CREATE UNIQUE INDEX ON pr_stoc.t_observations(carre_numnat, date, id_releve, point, codesp_euring, distance_v2)
;

-- alter table pr_stoc.observations
--   drop constraint observations_id_releve_fkey,
--   add constraint observations_id_releve_fkey
-- foreign key (id_releve)
-- references pr_stoc.releves (id)
-- on delete cascade
-- ;

/* Table des correspondances des distances */
--DROP TABLE pr_stoc.bib_code_distances;

CREATE TABLE pr_stoc.bib_code_distances (
    id                    BIGINT NOT NULL PRIMARY KEY,
    code                  VARCHAR(100),
    code_vn               VARCHAR(20),
    libelle               VARCHAR(200),
    defaut                VARCHAR(200),
    libelle_international VARCHAR(200)
);

ALTER TABLE pr_stoc.bib_code_distances
    OWNER TO geonature;

CREATE INDEX ON pr_stoc.bib_code_distances USING btree(id);
CREATE INDEX ON pr_stoc.bib_code_distances USING btree(code_vn);

ALTER TABLE pr_stoc.bib_code_points
    RENAME TO bib_code_points_old;

CREATE TABLE pr_stoc.bib_code_points (
    id         SERIAL PRIMARY KEY,
    type_code  VARCHAR(50),
    principal  VARCHAR(5),
    colonne    VARCHAR(5),
    code       VARCHAR(50),
    code_vn    VARCHAR(50),
    libelle    VARCHAR(200),
    libelle_vn VARCHAR(100)
)
;

CREATE INDEX bib_code_points_type_code_idx
    ON pr_stoc.bib_code_points(type_code)
;

CREATE INDEX bib_code_points_code_idx
    ON pr_stoc.bib_code_points(code)
;

CREATE INDEX bib_code_points_libelle_idx
    ON pr_stoc.bib_code_points(libelle)
;

CREATE INDEX bib_code_points_libelle_vn_idx
    ON pr_stoc.bib_code_points(libelle_vn)
;
