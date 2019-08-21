/***************************************************************************
 * Triggers déclenchés sur les tables de réception des données VisioNature *
 ***************************************************************************/

/* Retrieve survey values from  visionature codes  (habitat, meteo, etc.)  */

DROP FUNCTION IF EXISTS pr_stoc.get_code_point_values_from_vn_code;
CREATE OR REPLACE FUNCTION pr_stoc.get_code_point_values_from_vn_code(field_name ANYELEMENT, vn_code text, OUT result ANYELEMENT)
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


CREATE OR REPLACE FUNCTION pr_stoc.delete_releves() RETURNS TRIGGER AS
$$
BEGIN
    RAISE NOTICE 'TG_OP %', tg_op;
    -- Deleting data on src_vn.observations when raw data is deleted
    DELETE
    FROM pr_stoc.t_releves
    WHERE (carre_numnat, date, point_num) = (old.item ;
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
    the_date date,
     the_heure        TIME,
        the_observateur  VARCHAR(100),
        the_carre_numnat INTEGER,
        the_point_num    INTEGER,
        the_altitude     INTEGER,
        the_nuage        INTEGER,
        the_pluie        INTEGER,
        the_vent         INTEGER,
        the_visibilite   INTEGER,
        the_p_milieu     VARCHAR(10),
        the_p_type       VARCHAR(10),
        the_p_cat1       VARCHAR(10),
        the_p_cat2       VARCHAR(10),
        the_p_ss_cat1    VARCHAR(10),
        the_p_ss_cat2    VARCHAR(10),
        the_s_milieu     VARCHAR(10),
        the_s_type       VARCHAR(10),
        the_s_cat1       VARCHAR(10),
        the_s_cat2       VARCHAR(10),
        the_s_ss_cat1    VARCHAR(10),
        the_s_ss_cat2    VARCHAR(10),
        the_site         BOOLEAN,
        the_geom         geometry(point, 2154),
        the_passage_mnhn VARCHAR(10),
        the_id_source    TEXT[]
BEGIN



END;
$$ LANGUAGE plpgsql;
