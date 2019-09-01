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

/* Table dictionnaire des codes espèces */
/*DROP TABLE IF EXISTS pr_stoc.cor_euring_vn_taxref;*/
CREATE TABLE pr_stoc.cor_euring_vn_taxref (
    id            SERIAL PRIMARY KEY,
    code_euring   VARCHAR(20),
    num_euring    INTEGER,
    taxref_cd_nom INTEGER,
    vn_id_species INTEGER
);

CREATE INDEX cor_euring_vn_taxref_idx_code_euring ON pr_stoc.cor_euring_vn_taxref(code_euring);
CREATE INDEX cor_euring_vn_taxref_idx_cd_nom ON pr_stoc.cor_euring_vn_taxref(taxref_cd_nom);
CREATE INDEX cor_euring_vn_taxref_idx_vn_id ON pr_stoc.cor_euring_vn_taxref(vn_id_species);


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


/* Table des relevés */
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
    source_id           INTEGER,
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
    id                  SERIAL NOT NULL PRIMARY KEY,
    id_releve           INTEGER REFERENCES pr_stoc.t_releves(id)
        ON DELETE CASCADE,
    codesp_euring       VARCHAR(10),
    vn_is_species       INTEGER,
    nombre              INTEGER,
    distance            VARCHAR(50),
    details             JSONB,
    source_bdd          VARCHAR(50),
    source_id           INTEGER,
    source_id_universal VARCHAR(50)
)
;

-- alter table pr_stoc.observations
--   drop constraint observations_id_releve_fkey,
--   add constraint observations_id_releve_fkey
-- foreign key (id_releve)
-- references pr_stoc.releves (id)
-- on delete cascade
-- ;
