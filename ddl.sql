/*******************************************************************************************
 * Créatiop d''une bdd de centralisation des données STOC depuis les portails VisioNature  *
 *******************************************************************************************/

CREATE SCHEMA IF NOT EXISTS pr_stoc AUTHORIZATION geonature;

CREATE TABLE IF NOT EXISTS pr_stoc.l_grille
    (
        id        integer NOT NULL PRIMARY KEY,
        area      float,
        perimeter float,
        carrenat  integer,
        numnat    integer,
        x_coord   float,
        y_coord   float,
        geom      geometry(Polygon, 2154)
    );

ALTER TABLE pr_stoc.l_grille
    OWNER TO geonature;

CREATE INDEX l_grille_geom_idx
    ON pr_stoc.l_grille USING gist (geom);


ALTER TABLE pr_stoc.l_grille
    ADD CONSTRAINT l_grille_numnat UNIQUE (numnat)
;


/* Table des points */
-- drop table if exists pr_stoc.t_releves cascade;
CREATE TABLE pr_stoc.t_releves
    (
        id           serial NOT NULL PRIMARY KEY,
        date         date,
        heure        time,
        observateur  varchar(100),
        carre_numnat integer,
        point_num    integer,
        altitude     integer,
        nuage        integer,
        pluie        integer,
        vent         integer,
        visibilite   integer,
        p_milieu     varchar(10),
        p_type       varchar(10),
        p_cat1       varchar(10),
        p_cat2       varchar(10),
        p_ss_cat1    varchar(10),
        p_ss_cat2    varchar(10),
        s_milieu     varchar(10),
        s_type       varchar(10),
        s_cat1       varchar(10),
        s_cat2       varchar(10),
        s_ss_cat1    varchar(10),
        s_ss_cat2    varchar(10),
        site         boolean,
        geom         geometry(point, 2154),
        passage_mnhn varchar(10),
        source_bdd   varchar(50),
        source_id    text[],
        source_id_universal varchar(50) unique
    )
;


/* Index sur les colonnes carre_numnat, date et point_num */
CREATE UNIQUE INDEX ON pr_stoc.t_releves (carre_numnat, date, point_num)
;

CREATE INDEX ON pr_stoc.t_releves
    USING gist (geom)
;


/* Table des observations */
-- drop table if exists pr_stoc.observations cascade
-- ;

CREATE TABLE pr_stoc.t_observations
    (
        id            serial NOT NULL PRIMARY KEY,
        carre_numnat  integer,
        date          date,
        time          time,
        id_releve     integer REFERENCES pr_stoc.t_releves (id)
            ON DELETE CASCADE,
        point         integer,
        codesp_euring varchar(10),
        nombre        integer,
        source        varchar(20),
        passage       varchar(20),
        distance_v2   varchar(50),
        id_source     text[]
    )
;

CREATE UNIQUE INDEX ON pr_stoc.t_observations (carre_numnat, date, id_releve, point, codesp_euring, distance_v2)
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

CREATE TABLE pr_stoc.bib_code_distances
    (
        id                    bigint NOT NULL PRIMARY KEY,
        code                  varchar(100),
        code_vn               varchar(20),
        libelle               varchar(200),
        defaut                varchar(200),
        libelle_international varchar(200)
    );

ALTER TABLE pr_stoc.bib_code_distances
    OWNER TO geonature;

CREATE INDEX ON pr_stoc.bib_code_distances USING btree (id);
CREATE INDEX ON pr_stoc.bib_code_distances USING btree (code_vn);

ALTER TABLE pr_stoc.bib_code_points
    RENAME TO bib_code_points_old;

CREATE TABLE pr_stoc.bib_code_points
    (
        id         serial PRIMARY KEY,
        type_code  varchar(50),
        principal  varchar(5),
        colonne    varchar(5),
        code       varchar(50),
        code_vn    varchar(50),
        libelle    varchar(200),
        libelle_vn varchar(100)
    )
;

CREATE INDEX bib_code_points_type_code_idx
    ON pr_stoc.bib_code_points (type_code)
;

CREATE INDEX bib_code_points_code_idx
    ON pr_stoc.bib_code_points (code)
;

CREATE INDEX bib_code_points_libelle_idx
    ON pr_stoc.bib_code_points (libelle)
;

CREATE INDEX bib_code_points_libelle_vn_idx
    ON pr_stoc.bib_code_points (libelle_vn)
;
