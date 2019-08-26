/***************************************************************************
 * Triggers déclenchés sur les tables de réception des données VisioNature *
 ***************************************************************************/

/* Retrieve survey values from  visionature codes  (habitat, meteo, etc.)  */

DROP FUNCTION IF EXISTS pr_stoc.get_code_point_values_from_vn_code;
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
$$ LANGUAGE plpgsql;


/* TEST:
   SELECT pr_stoc.get_code_point_values_from_vn_code('principal'::text, 'E1_1') as pr,
       pr_stoc.get_code_point_values_from_vn_code('colonne'::text, 'E1_1') as col,
       pr_stoc.get_code_point_values_from_vn_code('code'::text, 'E1_1') as code;
RESULTAT:
    pr	col	code
    E	1	1
*/

/* Retrieve distances values from  visionature codes  (distance)  */

DROP FUNCTION IF EXISTS pr_stoc.get_distance_label_from_vn_code;
CREATE OR REPLACE FUNCTION pr_stoc.get_distance_label_from_vn_code(vn_code TEXT, OUT result TEXT)
    RETURNS TEXT AS
$$
BEGIN
    EXECUTE format(
            'SELECT bib_code_distances.libelle from pr_stoc.bib_code_distances where code_vn = $1 limit 1')
        INTO result
        USING vn_code;
END ;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS pr_stoc.get_altitude_from_dem;
CREATE OR REPLACE FUNCTION pr_stoc.get_altitude_from_dem(geom GEOMETRY(POINT, 2154), OUT result INT)
    RETURNS INT AS
$$
BEGIN
    EXECUTE format(
            'SELECT st_value(rast, $1)::int from ref_geo.dem where st_intersects(rast, $1)')
        INTO result
        USING geom;
END ;
$$ LANGUAGE plpgsql;

/* TEST:
   select pr_stoc.get_distance_label_from_vn_code('LESS200') as dist;
   RESULTAT:
    dist
    100-200m
 */

CREATE OR REPLACE FUNCTION pr_stoc.delete_releves() RETURNS TRIGGER AS
$$
BEGIN
    RAISE NOTICE 'TG_OP %', tg_op;
    -- Deleting data on src_vn.observations when raw data is deleted
    DELETE
    FROM pr_stoc.t_releves
    WHERE (carre_numnat, date, point_num) = (old.item;
    RAISE NOTICE 'DELETE DATA % from %', old.id, old.site;
    IF NOT found
    THEN
        RETURN NULL;
    END IF;
    RETURN old;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pr_stoc.upsert_releves() RETURNS TRIGGER AS
$$
DECLARE
    the_date                DATE;
    the_heure               TIME;
    the_observateur         VARCHAR(100);
    the_carre_numnat        INTEGER;
    the_point_num           INTEGER;
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
BEGIN
    the_date = CAST(new.item ->> 'date_start' AS DATE);
    the_heure = CAST(new.item ->> 'time_start' AS TIME);
    the_observateur =
            src_lpodatas.get_observer_full_name_from_vn(
                    cast(new.item ->> '@uid' AS INT));
    the_carre_numnat = cast(new.item #>> '{protocol, site_code}' AS BIGINT);
    the_point_num = cast(new.item #>> '{protocol, sequence_number}' AS BIGINT);
    the_altitude = pr_stoc.get_altitude_from_dem(st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154));
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
    the_site = CASE WHEN new.item #>> '{protocol, site_code}' LIKE '99%'
                        THEN TRUE
                    ELSE FALSE END;
    the_geom = st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154);
    the_passage_mnhn = cast(new.item #>> '{protocol, visit_number}' AS INT);
    the_source_id_universal = new.item ->> 'id_form_universal';
    the_source_bdd = new.site;
    IF (TG_OP = 'UPDATE')
    THEN
        UPDATE pr_stoc.t_releves
        SET date                = the_date,
            heure               = the_heure,
            observateur         = the_observateur,
            carre_numnat        = the_carre_numnat,
            point_num           = the_point_num,
            altitude            = the_altitude,
            nuage               = the_nuage,
            pluie               = the_pluie,
            vent                = the_vent,
            visibilite          = the_visibilite,
            p_milieu            = the_p_milieu,
            p_type              = the_p_type,
            p_cat1              = the_p_cat1,
            p_cat2              = the_p_cat2,
            p_ss_cat1           = the_p_ss_cat1,
            p_ss_cat2           = the_p_ss_cat2,
            s_milieu            = the_s_milieu,
            s_type              = the_s_type,
            s_cat1              = the_s_cat1,
            s_cat2              = the_s_cat2,
            s_ss_cat1           = the_s_ss_cat1,
            s_ss_cat2           = the_s_ss_cat2,
            site                = the_site,
            geom                = the_geom,
            passage_mnhn        = the_passage_mnhn,
            source_bdd          = the_source_bdd,
            source_id_universal = the_source_id_universal
        WHERE (carre_numnat, date, point_num) = (the_carre_numnat, the_date, the_point_num);
        IF NOT found
        THEN
            INSERT INTO pr_stoc.t_releves (date,
                                           heure,
                                           observateur,
                                           carre_numnat,
                                           point_num,
                                           altitude,
                                           nuage,
                                           pluie,
                                           vent,
                                           visibilite,
                                           p_milieu,
                                           p_type,
                                           p_cat1,
                                           p_cat2,
                                           p_ss_cat1,
                                           p_ss_cat2,
                                           s_milieu,
                                           s_type,
                                           s_cat1,
                                           s_cat2,
                                           s_ss_cat1,
                                           s_ss_cat2,
                                           site,
                                           geom,
                                           passage_mnhn,
                                           source_bdd,
                                           source_id_universal)
            VALUES (the_date,
                    the_heure,
                    the_observateur,
                    the_carre_numnat,
                    the_point_num,
                    the_altitude,
                    the_nuage,
                    the_pluie,
                    the_vent,
                    the_visibilite,
                    the_p_milieu,
                    the_p_type,
                    the_p_cat1,
                    the_p_cat2,
                    the_p_ss_cat1,
                    the_p_ss_cat2,
                    the_s_milieu,
                    the_s_type,
                    the_s_cat1,
                    the_s_cat2,
                    the_s_ss_cat1,
                    the_s_ss_cat2,
                    the_site,
                    the_geom,
                    the_passage_mnhn,
                    the_source_bdd,
                    the_source_id_universal);
            RETURN new;
        END IF;
        RETURN new;
    ELSE
        IF (TG_OP = 'INSERT')
        THEN
            INSERT INTO pr_stoc.t_releves (date,
                                           heure,
                                           observateur,
                                           carre_numnat,
                                           point_num,
                                           altitude,
                                           nuage,
                                           pluie,
                                           vent,
                                           visibilite,
                                           p_milieu,
                                           p_type,
                                           p_cat1,
                                           p_cat2,
                                           p_ss_cat1,
                                           p_ss_cat2,
                                           s_milieu,
                                           s_type,
                                           s_cat1,
                                           s_cat2,
                                           s_ss_cat1,
                                           s_ss_cat2,
                                           site,
                                           geom,
                                           passage_mnhn,
                                           source_bdd,
                                           source_id_universal)
            VALUES (the_date,
                    the_heure,
                    the_observateur,
                    the_carre_numnat,
                    the_point_num,
                    the_altitude,
                    the_nuage,
                    the_pluie,
                    the_vent,
                    the_visibilite,
                    the_p_milieu,
                    the_p_type,
                    the_p_cat1,
                    the_p_cat2,
                    the_p_ss_cat1,
                    the_p_ss_cat2,
                    the_s_milieu,
                    the_s_type,
                    the_s_cat1,
                    the_s_cat2,
                    the_s_ss_cat1,
                    the_s_ss_cat2,
                    the_site,
                    the_geom,
                    the_passage_mnhn,
                    the_source_bdd,
                    the_source_id_universal)
            ON CONFLICT DO UPDATE SET date                = the_date,
                                      heure               = the_heure,
                                      observateur         = the_observateur,
                                      carre_numnat        = the_carre_numnat,
                                      point_num           = the_point_num,
                                      altitude            = the_altitude,
                                      nuage               = the_nuage,
                                      pluie               = the_pluie,
                                      vent                = the_vent,
                                      visibilite          = the_visibilite,
                                      p_milieu            = the_p_milieu,
                                      p_type              = the_p_type,
                                      p_cat1              = the_p_cat1,
                                      p_cat2              = the_p_cat2,
                                      p_ss_cat1           = the_p_ss_cat1,
                                      p_ss_cat2           = the_p_ss_cat2,
                                      s_milieu            = the_s_milieu,
                                      s_type              = the_s_type,
                                      s_cat1              = the_s_cat1,
                                      s_cat2              = the_s_cat2,
                                      s_ss_cat1           = the_s_ss_cat1,
                                      s_ss_cat2           = the_s_ss_cat2,
                                      site                = the_site,
                                      geom                = the_geom,
                                      passage_mnhn        = the_passage_mnhn,
                                      source_bdd          = the_source_bdd,
                                      source_id_universal = the_source_id_universal;
            RETURN new;
        END IF;
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;


/* trigger sur les relevés
   - Sur les relevés avec un id_form_universal null uniquement pour éviter les erreurs sur les archives stoc FEPS et MNHN
 */
DROP TRIGGER IF EXISTS stoc_releve_update_from_vn_trigger ON import_vn.forms_json;
CREATE TRIGGER stoc_releve_update_from_vn_trigger
    AFTER UPDATE OR INSERT
    ON import_vn.forms_json
    FOR EACH ROW
    WHEN (new.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS')
EXECUTE PROCEDURE pr_stoc.upsert_releves();

UPDATE import_vn.forms_json
SET site=site;

/* Trigger sur les observations, conditions:
   id_form_universal not null and id_form_universal in (select id_form_universal from pr_stoc.t_releves) */


