SELECT item ->> 'id_form_universal'
FROM import_vn.forms_json
WHERE item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT jsonb_pretty(item)
FROM import_vn.forms_json
WHERE item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';


SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) AS dist
FROM import_vn.observations_json o
         LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
{"age": "AD", "sex": "U", "count": "1", "distance": "LESS200", "condition": "U"}
{"age": "AD", "sex": "U", "count": "1", "distance": "LESS25", "condition": "U"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS100", "condition": "AUDIO"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS100", "condition": "FLY"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS100", "condition": "U"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS200", "condition": "AUDIO"}
*/

SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) ->> 'distance' AS dist
FROM import_vn.observations_json o
         LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
LESS100
LESS200
LESS25
MORE200
TRANSIT
 */

SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) ->> 'age' AS dist
FROM import_vn.observations_json o
         LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
AD
U
*/

SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) ->> 'sex' AS dist
FROM import_vn.observations_json o
         LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
U
*/

SELECT jsonb_pretty(item)
FROM import_vn.forms_json
WHERE item ->> 'id_form_universal' LIKE '65_469993';

SELECT date(to_timestamp(CAST(o.item #>> '{date,@timestamp}' AS DOUBLE PRECISION)))
       --(((o.item -> 'observers') -> 0 )-> 'details' ->0) ->> 'sex' as dist
FROM import_vn.observations_json o
         LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT jsonb_pretty(f.item)
FROM import_vn.forms_json f
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

CREATE TABLE tmp.observers_forms AS
SELECT DISTINCT o.id_form_universal, ((o.item -> 'observers') -> 0) -> '@uid'
FROM import_vn.observations_json o
WHERE o.id_form_universal IS NOT NULL;

WITH observ AS (SELECT DISTINCT o.id_form_universal, ((o.item -> 'observers') -> 0) -> '@uid' AS uid
                FROM import_vn.observations_json o
                WHERE o.id_form_universal IS NOT NULL)
UPDATE import_vn.forms_json f
SET item = jsonb_set(f.item, '{@uid}', observ.uid)
FROM observ
WHERE observ.id_form_universal = f.item ->> 'id_form_universal';

SELECT jsonb_pretty(f.item)
FROM import_vn.forms_json f
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT extract(DOY FROM cast(f.item ->> 'date_start' AS DATE))   AS doy
     , extract(dom FROM cast(f.item ->> 'date_start' AS DATE))   AS doy
     , extract(MONTH FROM cast(f.item ->> 'date_start' AS DATE)) AS doy
     , f.item #>> '{protocol, visit_number}'                     AS visitnum
FROM import_vn.forms_json f
WHERE f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS'
ORDER BY extract(DOY FROM cast(f.item ->> 'date_start' AS DATE)) DESC;


SELECT CAST(new.item ->> 'date_start' AS DATE)                                             AS the_date
     ,
    cast(new.item ->> 'time_start' AS TIME)                                                AS the_heure
     ,
    src_lpodatas.get_observer_full_name_from_vn(
            cast(item ->> '@uid' AS INT))                                                  AS the_observer
     ,
    cast(new.item #>> '{protocol, site_code}' AS BIGINT)                                   AS the_carre_numnat
     ,
    cast(new.item #>> '{protocol, sequence_number}' AS BIGINT)                             AS the_point_num
     ,
    pr_stoc.get_altitude_from_dem(st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154)) AS alti
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, stoc_cloud}')      AS the_nuage
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, stoc_rain}')       AS the_nuage
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, stoc_wind}')       AS the_nuage
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, stoc_visibility}') AS the_nuage
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hp1}')    AS the_p_milieu
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hp2}')    AS the_p_type
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hp3A}')   AS the_p_cat1
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hp3B}')   AS the_p_cat2
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hp4A}')   AS the_p_sscat1
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hp4B}')   AS the_p_sscat2
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hs1}')    AS the_s_milieu
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hs2}')    AS the_s_type
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hs3A}')   AS the_s_cat1
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hs3B}')   AS the_s_cat2
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hs4A}')   AS the_s_sscat1
     ,
    pr_stoc.get_code_point_values_from_vn_code('code'::TEXT,
                                               new.item #>> '{protocol, habitat, hs4B}')   AS the_s_sscat2
     ,
    CASE WHEN new.item #>> '{protocol, site_code}' LIKE '99%'
             THEN TRUE
         ELSE FALSE END                                                                    AS the_site
     ,
    st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154)                                                                       AS geom
     ,
    cast(new.item #>> '{protocol, visit_number}' AS INT)                                   AS the_passage_mnhn


--        the_passage_mnhn

--     the_site AS BOOLEAN;
--     the_geom AS geometry
--         (point; 2154);
--     the_passage_mnhn AS VARCHAR
--         (10);
--     the_id_source AS TEXT
--         []
FROM import_vn.forms_json AS new
WHERE item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT DISTINCT st_srid(geom)
FROM ref_geo.dem_vector;

SELECT pr_stoc.get_altitude_from_dem(st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154))
FROM import_vn.forms_json AS new;
SELECT st_value(dem.rast, st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154))
FROM ref_geo.dem st_transform
         (
             st_setsrid(st_makepoint(CAST (NEW.item ->> 'lon' AS FLOAT), CAST (NEW.item ->> 'lat' AS FLOAT)), 4326)
         , 2154)

SELECT DISTINCT cast(new.item ->> 'lon' AS FLOAT)
     , cast(new.item ->> 'lat' AS FLOAT)
     , st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154)
FROM import_vn.forms_json new;

CREATE TABLE lpoaura_fcl.forms_json AS
SELECT row_number() OVER () AS id
     , st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326),
        2154)               AS geom
FROM import_vn.forms_json new;

SELECT cast(item ->> '@uid' AS INT)
FROM import_vn.forms_json;

CAST
    ((item -> '@uid') AS INTEGER)
    FROM import_vn.forms_json;

select distinct st_srid(st_transform(
        st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154)) from import_vn.forms_json new;