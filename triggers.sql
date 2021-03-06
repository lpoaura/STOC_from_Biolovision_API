/***************************************************************************
 * Triggers déclenchés sur les tables de réception des données VisioNature *
 ***************************************************************************/

/* Retrieve survey values from  visionature codes  (habitat, meteo, etc.)  */

DROP FUNCTION IF EXISTS pr_stoc.get_code_point_values_from_vn_code
;

CREATE OR REPLACE FUNCTION pr_stoc.get_code_point_values_from_vn_code(field_name ANYELEMENT, vn_code TEXT, OUT result ANYELEMENT)
    RETURNS ANYELEMENT AS
$$
BEGIN
    EXECUTE format(
            'SELECT bib_code_points.%I from pr_stoc.bib_code_points where code_vn = $1 limit 1',
            field_name)
        INTO result
        USING vn_code;
END ;
$$
    LANGUAGE plpgsql
;


/* TEST:
   SELECT pr_stoc.get_code_point_values_from_vn_code('principal'::text, 'E1_1') as pr,
       pr_stoc.get_code_point_values_from_vn_code('colonne'::text, 'E1_1') as col,
       pr_stoc.get_code_point_values_from_vn_code('code'::text, 'E1_1') as code;
RESULTAT:
    pr	col	code
    E	1	1
*/

/* Retrieve distances values from  visionature codes  (distance)  */

DROP FUNCTION IF EXISTS pr_stoc.get_distance_label_from_vn_code
;

CREATE OR REPLACE FUNCTION pr_stoc.get_distance_label_from_vn_code(vn_code TEXT, OUT result TEXT)
    RETURNS TEXT AS
$$
BEGIN
    EXECUTE format(
            'SELECT bib_code_distances.libelle from pr_stoc.bib_code_distances where code_vn = $1 limit 1')
        INTO result
        USING vn_code;
END ;
$$
    LANGUAGE plpgsql
;

DROP FUNCTION IF EXISTS pr_stoc.get_altitude_from_dem
;

CREATE OR REPLACE FUNCTION pr_stoc.get_altitude_from_dem(geom GEOMETRY(POINT, 2154), OUT result INT)
    RETURNS INT AS
$$
BEGIN
    EXECUTE format(
            'SELECT st_value(rast, $1)::int from ref_geo.dem where st_intersects(rast, $1)')
        INTO result
        USING geom;
END ;
$$
    LANGUAGE plpgsql
;

/* Get EURING code from vn id_species */

DROP FUNCTION IF EXISTS pr_stoc.get_code_euring_from_vn_id_species
;

CREATE OR REPLACE FUNCTION pr_stoc.get_code_euring_from_vn_id_species(id_species INT, OUT result VARCHAR(20))
    RETURNS VARCHAR(20) AS
$$
BEGIN
    EXECUTE format(
            'SELECT cor_euring_vn_taxref.code_euring from pr_stoc.cor_euring_vn_taxref where vn_id_species = $1 and ref_tax is true limit 1')
        INTO result
        USING id_species;
END ;
$$
    LANGUAGE plpgsql
;

/* Get id_releve from vn id_form_universal */

DROP FUNCTION IF EXISTS pr_stoc.get_id_releve_from_id_form_uid
;

CREATE OR REPLACE FUNCTION pr_stoc.get_id_releve_from_id_form_uid(id_form_universal VARCHAR(50), OUT result INT)
    RETURNS INT AS
$$
BEGIN
    EXECUTE format(
            'SELECT t_releves.id from pr_stoc.t_releves where source_id_universal = $1 limit 1')
        INTO result
        USING id_form_universal;
END ;
$$
    LANGUAGE plpgsql
;


DROP FUNCTION IF EXISTS import_vn.forms_json_id_universal
;

CREATE OR REPLACE FUNCTION import_vn.forms_json_id_universal(JSONB)
    RETURNS VARCHAR(50)
AS
$$
SELECT ($1 -> 'id_form_universal')::VARCHAR(50)
$$
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
;

DROP FUNCTION IF EXISTS import_vn.forms_json_protocol_name
;

CREATE OR REPLACE FUNCTION import_vn.forms_json_protocol_name(JSONB)
    RETURNS VARCHAR(50)
AS
$$
SELECT ($1 #> '{protocol, protocol_name}')::VARCHAR(50)
$$
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
;

DROP FUNCTION IF EXISTS pr_stoc.is_stoc_eps_form
;

CREATE FUNCTION pr_stoc.is_stoc_eps_form(id_form_universal VARCHAR(50), protocol_name VARCHAR(20)) RETURNS BOOLEAN AS
$$
SELECT item #>> '{protocol, protocol_name}' ILIKE $2
FROM import_vn.forms_json
WHERE item ->> 'id_form_universal' LIKE $1
$$
    LANGUAGE sql
;



/*
select pr_stoc.get_code_euring_from_vn_id_species(518);
*/
CREATE OR REPLACE FUNCTION pr_stoc.delete_releves() RETURNS TRIGGER AS
$$
BEGIN
    -- Deleting data on src_vn.observations when raw data is deleted
    DELETE
    FROM pr_stoc.t_releves
    WHERE source_id_universal = old.item ->> 'id_form_universal';
    IF NOT found
    THEN
        RETURN NULL;
    END IF;
    RETURN old;
END;
$$
    LANGUAGE plpgsql
;

DROP TRIGGER IF EXISTS stoc_releve_delete_from_vn_trigger ON import_vn.forms_json
;

CREATE TRIGGER stoc_releve_delete_from_vn_trigger
    AFTER DELETE
    ON import_vn.forms_json
    FOR EACH ROW
    WHEN (old.item #>> '{protocol, protocol_name}' LIKE 'STOC_%')
EXECUTE PROCEDURE pr_stoc.delete_releves()
;

CREATE OR REPLACE FUNCTION pr_stoc.upsert_releves() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    the_date                DATE;
    the_heure               TIME;
    the_observateur         VARCHAR(100);
    the_carre_numnat        INTEGER;
    the_point_num           INTEGER;
    the_site_name           VARCHAR(250);
    the_altitude            INTEGER;
    the_nuage               INTEGER;
    the_pluie               INTEGER;
    the_vent                INTEGER;
    the_visibilite          INTEGER;
    the_p_milieu            VARCHAR(10);
    the_p_type              VARCHAR(10);
    the_p_cat1              VARCHAR(10);
    the_p_cat2              VARCHAR(10);
    the_p_ss_cat1           VARCHAR(10);
    the_p_ss_cat2           VARCHAR(10);
    the_s_milieu            VARCHAR(10);
    the_s_type              VARCHAR(10);
    the_s_cat1              VARCHAR(10);
    the_s_cat2              VARCHAR(10);
    the_s_ss_cat1           VARCHAR(10);
    the_s_ss_cat2           VARCHAR(10);
    the_site                BOOLEAN;
    the_geom                GEOMETRY(point, 2154);
    the_passage_mnhn        VARCHAR(10);
    the_source_id_universal VARCHAR(20);
    the_source_bdd          VARCHAR(20);
    the_type_eps            VARCHAR(20);
BEGIN
    the_date = cast(new.item ->> 'date_start' AS DATE);
    the_heure = cast(new.item ->> 'time_start' AS TIME);
    the_observateur =
            src_lpodatas.get_observer_full_name_from_vn(
                    cast(new.item ->> '@uid' AS INT));
    the_carre_numnat = CASE
                           WHEN new.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS'
                               THEN cast(new.item #>> '{protocol, site_code}' AS BIGINT)
        END;
    the_point_num = cast(new.item #>> '{protocol, sequence_number}' AS BIGINT);
    the_site_name = CASE
                        WHEN new.item #>> '{protocol, protocol_name}' LIKE 'STOC_SITES'
                            THEN new.item #>> '{protocol, site_code}'
        END;
    the_altitude = pr_stoc.get_altitude_from_dem(st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326),
            2154));
    the_nuage = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, stoc_cloud}');
    the_pluie = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, stoc_rain}');
    the_vent = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, stoc_wind}');
    the_visibilite =
            pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, stoc_visibility}');
    the_p_milieu = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hp1}');
    the_p_type = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hp2}');
    the_p_cat1 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hp3A}');
    the_p_cat2 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hp3B}');
    the_p_ss_cat1 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hp4A}');
    the_p_ss_cat2 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hp4B}');
    the_s_milieu = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hs1}');
    the_s_type = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hs2}');
    the_s_cat1 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hs3A}');
    the_s_cat2 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hs3B}');
    the_s_ss_cat1 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hs4A}');
    the_s_ss_cat2 = pr_stoc.get_code_point_values_from_vn_code('code'::TEXT, new.item #>> '{protocol, habitat, hs4B}');
    the_site = CASE
                   WHEN new.item #>> '{protocol, protocol_name}' LIKE 'STOC_SITES'
                       THEN TRUE
                   ELSE FALSE END;
    the_geom = st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154);
    the_passage_mnhn = cast(new.item #>> '{protocol, visit_number}' AS INT);
    the_source_id_universal = new.item ->> 'id_form_universal';
    the_source_bdd = new.site;
    the_type_eps = CASE
                       WHEN (new.item -> 'protocol' ? 'stoc_transport')
                           THEN 'Transect' END;
    IF (tg_op = 'UPDATE')
    THEN
        UPDATE pr_stoc.t_releves
        SET date                = the_date
          , heure               = the_heure
          , observateur         = the_observateur
          , carre_numnat        = the_carre_numnat
          , point_num           = the_point_num
          , site_name           = the_site_name
          , altitude            = the_altitude
          , nuage               = the_nuage
          , pluie               = the_pluie
          , vent                = the_vent
          , visibilite          = the_visibilite
          , p_milieu            = the_p_milieu
          , p_type              = the_p_type
          , p_cat1              = the_p_cat1
          , p_cat2              = the_p_cat2
          , p_ss_cat1           = the_p_ss_cat1
          , p_ss_cat2           = the_p_ss_cat2
          , s_milieu            = the_s_milieu
          , s_type              = the_s_type
          , s_cat1              = the_s_cat1
          , s_cat2              = the_s_cat2
          , s_ss_cat1           = the_s_ss_cat1
          , s_ss_cat2           = the_s_ss_cat2
          , site                = the_site
          , geom                = the_geom
          , passage_mnhn        = the_passage_mnhn
          , source_bdd          = the_source_bdd
          , source_id_universal = the_source_id_universal
          , type_eps            = the_type_eps
        WHERE
--                 (t_releves.carre_numnat, t_releves.date, t_releves.point_num, t_releves.source_id_universal) =
--                 (the_carre_numnat, the_date, the_point_num, the_source_id_universal);
t_releves.source_id_universal = the_source_id_universal;
        IF NOT found
        THEN
            INSERT INTO pr_stoc.t_releves ( date
                                          , heure
                                          , observateur
                                          , carre_numnat
                                          , point_num
                                          , site_name
                                          , altitude
                                          , nuage
                                          , pluie
                                          , vent
                                          , visibilite
                                          , p_milieu
                                          , p_type
                                          , p_cat1
                                          , p_cat2
                                          , p_ss_cat1
                                          , p_ss_cat2
                                          , s_milieu
                                          , s_type
                                          , s_cat1
                                          , s_cat2
                                          , s_ss_cat1
                                          , s_ss_cat2
                                          , site
                                          , geom
                                          , passage_mnhn
                                          , source_bdd
                                          , source_id_universal
                                          , type_eps)
            VALUES ( the_date
                   , the_heure
                   , the_observateur
                   , the_carre_numnat
                   , the_point_num
                   , the_site_name
                   , the_altitude
                   , the_nuage
                   , the_pluie
                   , the_vent
                   , the_visibilite
                   , the_p_milieu
                   , the_p_type
                   , the_p_cat1
                   , the_p_cat2
                   , the_p_ss_cat1
                   , the_p_ss_cat2
                   , the_s_milieu
                   , the_s_type
                   , the_s_cat1
                   , the_s_cat2
                   , the_s_ss_cat1
                   , the_s_ss_cat2
                   , the_site
                   , the_geom
                   , the_passage_mnhn
                   , the_source_bdd
                   , the_source_id_universal
                   , the_type_eps);
            RETURN new;
        END IF;
        RETURN new;
    ELSE
        IF (tg_op = 'INSERT')
        THEN
            INSERT INTO pr_stoc.t_releves ( date
                                          , heure
                                          , observateur
                                          , carre_numnat
                                          , point_num
                                          , site_name
                                          , altitude
                                          , nuage
                                          , pluie
                                          , vent
                                          , visibilite
                                          , p_milieu
                                          , p_type
                                          , p_cat1
                                          , p_cat2
                                          , p_ss_cat1
                                          , p_ss_cat2
                                          , s_milieu
                                          , s_type
                                          , s_cat1
                                          , s_cat2
                                          , s_ss_cat1
                                          , s_ss_cat2
                                          , site
                                          , geom
                                          , passage_mnhn
                                          , source_bdd
                                          , source_id_universal
                                          , type_eps)
            VALUES ( the_date
                   , the_heure
                   , the_observateur
                   , the_carre_numnat
                   , the_point_num
                   , the_site_name
                   , the_altitude
                   , the_nuage
                   , the_pluie
                   , the_vent
                   , the_visibilite
                   , the_p_milieu
                   , the_p_type
                   , the_p_cat1
                   , the_p_cat2
                   , the_p_ss_cat1
                   , the_p_ss_cat2
                   , the_s_milieu
                   , the_s_type
                   , the_s_cat1
                   , the_s_cat2
                   , the_s_ss_cat1
                   , the_s_ss_cat2
                   , the_site
                   , the_geom
                   , the_passage_mnhn
                   , the_source_bdd
                   , the_source_id_universal
                   , the_type_eps)
            ON CONFLICT (source_id_universal) DO UPDATE SET date                = the_date
                                                          , heure               = the_heure
                                                          , observateur         = the_observateur
                                                          , carre_numnat        = the_carre_numnat
                                                          , point_num           = the_point_num
                                                          , site_name           = the_site_name
                                                          , altitude            = the_altitude
                                                          , nuage               = the_nuage
                                                          , pluie               = the_pluie
                                                          , vent                = the_vent
                                                          , visibilite          = the_visibilite
                                                          , p_milieu            = the_p_milieu
                                                          , p_type              = the_p_type
                                                          , p_cat1              = the_p_cat1
                                                          , p_cat2              = the_p_cat2
                                                          , p_ss_cat1           = the_p_ss_cat1
                                                          , p_ss_cat2           = the_p_ss_cat2
                                                          , s_milieu            = the_s_milieu
                                                          , s_type              = the_s_type
                                                          , s_cat1              = the_s_cat1
                                                          , s_cat2              = the_s_cat2
                                                          , s_ss_cat1           = the_s_ss_cat1
                                                          , s_ss_cat2           = the_s_ss_cat2
                                                          , site                = the_site
                                                          , geom                = the_geom
                                                          , passage_mnhn        = the_passage_mnhn
                                                          , source_bdd          = the_source_bdd
                                                          , source_id_universal = the_source_id_universal
                                                          , type_eps            = the_type_eps
            WHERE t_releves.source_id_universal = the_source_id_universal;
            --                         (t_releves.carre_numnat, t_releves.date, t_releves.point_num, t_releves.source_id_universal) =
--                         (the_carre_numnat, the_date, the_point_num, the_source_id_universal);

            RETURN new;
        END IF;
        RETURN new;
    END IF;
END;
$$
;


/* trigger sur les relevés
   - Sur les relevés avec un id_form_universal null uniquement pour éviter les erreurs sur les archives stoc FEPS et MNHN
 */
DROP TRIGGER IF EXISTS stoc_releve_upsert_from_vn_trigger ON import_vn.forms_json
;

CREATE TRIGGER stoc_releve_upsert_from_vn_trigger
    AFTER UPDATE OR INSERT
    ON import_vn.forms_json
    FOR EACH ROW
    WHEN (new.item #>> '{protocol, protocol_name}' LIKE 'STOC_%')
EXECUTE PROCEDURE pr_stoc.upsert_releves()
;


/* Trigger sur les observations pour détecter les types de relevés (transect ou point d'écoute) */

CREATE OR REPLACE FUNCTION pr_stoc.update_type_releves_from_obs() RETURNS TRIGGER AS
$$
DECLARE
    is_stoc_eps BOOLEAN;
BEGIN
    is_stoc_eps = new.id_form_universal IN (SELECT source_id_universal FROM pr_stoc.t_releves WHERE type_eps IS NULL);
    IF is_stoc_eps
    THEN
        UPDATE pr_stoc.t_releves
        SET type_eps = CASE
                           WHEN new.item #>> '{observers,0,precision}' LIKE 'transect%' THEN 'Transect'
                           ELSE 'Point'
            END
        WHERE source_id_universal = new.id_form_universal
          AND coalesce(type_eps, '') NOT LIKE 'Point';
    END IF;
    RETURN new;
END ;
$$
    LANGUAGE plpgsql
;

DROP TRIGGER IF EXISTS stoc_releve_update_type_eps_from_vn_trigger
    ON import_vn.observations_json
;

CREATE TRIGGER stoc_releve_update_type_eps_from_vn_trigger
    AFTER UPDATE OR INSERT
    ON import_vn.observations_json
    FOR EACH ROW
    --     WHEN pr_stoc.is_stoc_eps_form(new.item, 'STOC_EPS')
    WHEN (new.id_form_universal IS NOT NULL)
EXECUTE PROCEDURE pr_stoc.update_type_releves_from_obs()
;

/* Trigger sur les observations, conditions:
   id_form_universal not null and id_form_universal in (select id_form_universal from pr_stoc.t_releves) */

CREATE OR REPLACE FUNCTION pr_stoc.delete_obs_from_vn() RETURNS TRIGGER AS
$$
BEGIN
    DELETE
    FROM pr_stoc.t_observations
    WHERE (source_bdd, source_id) = (old.site, old.id);
    IF NOT found
    THEN
        RETURN NULL;
    END IF;
    RETURN old;
END;
$$
    LANGUAGE plpgsql
;

CREATE TRIGGER stoc_observation_delete_from_vn_trigger
    AFTER DELETE
    ON import_vn.observations_json
    FOR EACH ROW
    WHEN (old.id_form_universal IS NOT NULL)
EXECUTE PROCEDURE pr_stoc.delete_obs_from_vn()
;


CREATE OR REPLACE FUNCTION pr_stoc.upsert_obs_from_vn() RETURNS TRIGGER AS
$$
DECLARE
    the_id_releve     INT;
    the_codesp_euring VARCHAR(20);
    is_stoc_eps       BOOLEAN;
    the_obs           RECORD;
BEGIN
    is_stoc_eps = new.id_form_universal IN (SELECT source_id_universal FROM pr_stoc.t_releves);
    SELECT new INTO the_obs;
    RAISE DEBUG 'THE OBS %', the_obs;
    IF is_stoc_eps
    THEN
        the_id_releve = pr_stoc.get_id_releve_from_id_form_uid(new.id_form_universal);
        RAISE DEBUG '%, %, %', new.site, new.id_form_universal, the_id_releve;
        the_codesp_euring = pr_stoc.get_code_euring_from_vn_id_species(cast(new.item #>> '{species, @id}' AS INT));
        RAISE DEBUG 'EURING %', the_codesp_euring;
        IF (tg_op = 'UPDATE')
        THEN
            RAISE DEBUG 'DELETE DATA FROM RELEVE % WITH EURING %', the_id_releve, the_codesp_euring;
            DELETE
            FROM pr_stoc.t_observations
            WHERE id_releve = the_id_releve
              AND codesp_euring = the_codesp_euring;
        END IF;
        RAISE DEBUG 'INSERT DATA FROM RELEVE %', pr_stoc.get_id_releve_from_id_form_uid(new.id_form_universal);
        RAISE DEBUG 'DATA %', new.item #>> '{observers,0,id_universal}';
        WITH obs(id_releve, codesp_euring, species, details, source_bdd, source_id, source_id_universal, update_ts)
                 AS (VALUES ( the_id_releve
                            , the_codesp_euring
                            , cast(new.item #>> '{species, @id}' AS INT)
                            , new.item #> '{observers,0,details}'
                            , new.site
                            , new.id
                            , new.item #>> '{observers,0,id_universal}'
                            , to_timestamp(new.update_ts)))
        INSERT
        INTO pr_stoc.t_observations (id_releve, codesp_euring, vn_is_species, nombre, distance, details,
                                     source_bdd, source_id, source_id_universal, update_ts)
        SELECT obs.id_releve                                                      AS id_releve
             , obs.codesp_euring                                                  AS codesp_euring
             , obs.species                                                        AS species
             , (detail.obj ->> 'count')::INT                                      AS nombre
             , pr_stoc.get_distance_label_from_vn_code(detail.obj ->> 'distance') AS dist
             , detail.obj                                                         AS details
             , obs.source_bdd                                                     AS source_bdd
             , obs.source_id                                                      AS source_id
             , obs.source_id_universal                                            AS source_id_universal
             , obs.update_ts                                                      AS update_ts
        FROM obs
                 LEFT JOIN LATERAL jsonb_array_elements(obs.details) AS detail (obj)
                           ON TRUE;
    END IF;
    RETURN NULL;
END ;
$$
    LANGUAGE plpgsql
;

DROP TRIGGER IF EXISTS stoc_observation_upsert_from_vn_trigger
    ON import_vn.observations_json
;

CREATE TRIGGER stoc_observation_upsert_from_vn_trigger
    AFTER UPDATE OR INSERT
    ON import_vn.observations_json
    FOR EACH ROW
--     WHEN pr_stoc.is_stoc_eps_form(new.item, 'STOC_EPS')
    WHEN (new.id_form_universal IS NOT NULL)
EXECUTE PROCEDURE pr_stoc.upsert_obs_from_vn()
;
