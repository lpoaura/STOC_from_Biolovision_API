SELECT item ->> 'id_form_universal'
FROM import_vn.forms_json
WHERE
    item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT jsonb_pretty(item)
FROM import_vn.forms_json
WHERE
    item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';


SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) AS dist
FROM
    import_vn.observations_json o
        LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
{"age": "AD", "sex": "U", "count": "1", "distance": "LESS200", "condition": "U"}
{"age": "AD", "sex": "U", "count": "1", "distance": "LESS25", "condition": "U"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS100", "condition": "AUDIO"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS100", "condition": "FLY"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS100", "condition": "U"}
{"age": "U", "sex": "U", "count": "1", "distance": "LESS200", "condition": "AUDIO"}
*/

SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) ->> 'distance' AS dist
FROM
    import_vn.observations_json o
        LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
LESS100
LESS200
LESS25
MORE200
TRANSIT
 */

SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) ->> 'age' AS dist
FROM
    import_vn.observations_json o
        LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
AD
U
*/

SELECT DISTINCT (((o.item -> 'observers') -> 0) -> 'details' -> 0) ->> 'sex' AS dist
FROM
    import_vn.observations_json o
        LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

/*
U
*/

SELECT jsonb_pretty(item)
FROM import_vn.forms_json
WHERE
    item ->> 'id_form_universal' LIKE '65_469993';

SELECT
    date(to_timestamp(CAST(o.item #>> '{date,@timestamp}' AS DOUBLE PRECISION)))
    --(((o.item -> 'observers') -> 0 )-> 'details' ->0) ->> 'sex' as dist
FROM
    import_vn.observations_json o
        LEFT JOIN import_vn.forms_json f ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT jsonb_pretty(f.item)
FROM import_vn.forms_json f
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

CREATE TABLE tmp.observers_forms AS
SELECT DISTINCT o.id_form_universal, ((o.item -> 'observers') -> 0) -> '@uid'
FROM import_vn.observations_json o
WHERE
    o.id_form_universal IS NOT NULL;

WITH observ AS (SELECT DISTINCT o.id_form_universal, ((o.item -> 'observers') -> 0) -> '@uid' AS uid
                FROM import_vn.observations_json o
                WHERE o.id_form_universal IS NOT NULL)
UPDATE import_vn.forms_json f
SET
    item = jsonb_set(f.item, '{@uid}', observ.uid)
FROM observ
WHERE
    observ.id_form_universal = f.item ->> 'id_form_universal';

SELECT jsonb_pretty(f.item)
FROM import_vn.forms_json f
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT
    extract(DOY FROM cast(f.item ->> 'date_start' AS DATE))   AS doy
  , extract(dom FROM cast(f.item ->> 'date_start' AS DATE))   AS doy
  , extract(MONTH FROM cast(f.item ->> 'date_start' AS DATE)) AS doy
  , f.item #>> '{protocol, visit_number}'                     AS visitnum
FROM import_vn.forms_json f
WHERE
    f.item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS'
ORDER BY
    extract(DOY FROM cast(f.item ->> 'date_start' AS DATE)) DESC;


SELECT
    CAST(new.item ->> 'date_start' AS DATE)                                                AS the_date
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
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326),
            2154))                                                                         AS alti
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
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326),
            2154)                                                                          AS geom
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
WHERE
    item #>> '{protocol, protocol_name}' LIKE 'STOC_EPS';

SELECT DISTINCT st_srid(geom)
FROM ref_geo.dem_vector;

SELECT
    pr_stoc.get_altitude_from_dem(st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154))
FROM import_vn.forms_json AS new;
SELECT
    st_value(dem.rast, st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154))
FROM
    ref_geo.dem st_transform
        (
            st_setsrid(st_makepoint(CAST (NEW.item ->> 'lon' AS FLOAT), CAST (NEW.item ->> 'lat' AS FLOAT)), 4326)
        , 2154)

SELECT DISTINCT
    cast(new.item ->> 'lon' AS FLOAT)
  , cast(new.item ->> 'lat' AS FLOAT)
  , st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154)
FROM import_vn.forms_json new;

CREATE TABLE lpoaura_fcl.forms_json AS
SELECT
    row_number() OVER () AS id
  , st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326),
            2154)        AS geom
FROM import_vn.forms_json new;

SELECT cast(item ->> '@uid' AS INT)
FROM import_vn.forms_json;

CAST
    ((item -> '@uid') AS INTEGER)
    FROM import_vn.forms_json;

SELECT DISTINCT
    st_srid(st_transform(
            st_setsrid(st_makepoint(cast(new.item ->> 'lon' AS FLOAT), cast(new.item ->> 'lat' AS FLOAT)), 4326), 2154))
FROM import_vn.forms_json new;

CREATE TABLE tmp.forms_arch AS
SELECT *
FROM import_vn.forms_json
WHERE
    (cast(forms_json.item ->> '@uid' AS INT) = 11095)

/* Recherche de formulaires STOC en doublon */
WITH t1 AS
         (SELECT DISTINCT
              array_agg(DISTINCT item ->> 'id_form_universal') AS id_form_universal
            ,
              array_agg(DISTINCT src_lpodatas.get_observer_full_name_from_vn(
                      cast(item ->> '@uid' AS INT)))           AS observers
            ,
              item #>> '{protocol, site_code}'                 AS carre
            ,
              item ->> 'date_start'                            AS date
            ,
              item #>> '{protocol, sequence_number}'           AS pt
            ,
              count(*)
            ,
              bool_or(item -> 'protocol' ? 'stoc_transport')
          FROM import_vn.forms_json
          WHERE
                  item #>> '{protocol, protocol_name}' LIKE
                  'STOC_EPS'
          GROUP BY /*item ->> 'id_form_universal'
                 ,*/
              item #>> '{protocol, site_code}'
            ,
              item ->> 'date_start'
            ,
              item #>> '{protocol, sequence_number}'
          HAVING count(*) > 1)
SELECT *
FROM import_vn.forms_json
WHERE
        item #>> '{protocol, protocol_name}' LIKE
        'STOC_EPS' AND
        item ->>
        'id_form_universal' NOT IN
        (SELECT source_id_universal
         FROM pr_stoc.t_releves) AND
        (concat(CAST(item #>>
                     '{protocol, site_code}' AS BIGINT),
                '|',
                CAST(item ->>
                     'date_start' AS DATE),
                '|',
                CAST(item #>>
                     '{protocol, sequence_number}' AS BIGINT))) NOT IN
        (SELECT
             concat(carre_numnat,
                    '|', date,
                    '|', point_num)
         FROM pr_stoc.t_releves);

UPDATE import_vn.forms_json
SET
    site=site;

SELECT extract(EPOCH FROM now());

SELECT DISTINCT ((o.item -> 'observers') -> 0) ->> 'precision', count(*)
FROM
    import_vn.forms_json f
        LEFT JOIN import_vn.observations_json o ON o.id_form_universal = f.item ->> 'id_form_universal'
WHERE
    f.item ->> 'id_form_universal' IS NOT NULL
GROUP BY
    ((o.item -> 'observers') -> 0) ->> 'precision';

SELECT
    r.carre_numnat
  , r.date
  , r.passage_mnhn
  , r.source_id_universal
  , CASE WHEN (o.item -> 'observers') -> 0 ->> 'precision' LIKE 'subplace'
             THEN 'Point'
         WHEN (o.item -> 'observers') -> 0 ->> 'precision' LIKE 'transect'
             THEN 'Transect'
         ELSE NULL END AS type
FROM
    pr_stoc.t_releves r
        JOIN import_vn.observations_json o ON o.id_form_universal = r.source_id_universal

/* Select * from */
SET TIMEZONE = 'Europe/Paris';
WITH d AS (
    SELECT
        pr_stoc.get_id_releve_from_id_form_uid(o.id_form_universal)                                 AS id_releve
      , o.site                                                                                      AS source_bdd
      , o.id                                                                                        AS source_id
      , o.id_form_universal                                                                         AS id_form_universal
      , pr_stoc.get_code_euring_from_vn_id_species(cast(o.item #>> '{species, @id}' AS INT))        AS codesp_euring
      , o.item #>> '{species, @id}'                                                                 AS species
      , jsonb_array_elements((o.item -> 'observers') -> 0 -> 'details')                             AS details
      , jsonb_array_elements((o.item -> 'observers') -> 0 -> 'details') ->> 'count'                 AS nombre
      , pr_stoc.get_distance_label_from_vn_code(
                    jsonb_array_elements((o.item -> 'observers') -> 0 -> 'details') ->> 'distance') AS dist
    FROM import_vn.observations_json o
    WHERE
            id_form_universal IN
            (SELECT source_id_universal FROM pr_stoc.t_releves ORDER BY source_id_universal LIMIT 1) AND
            jsonb_array_length((item -> 'observers') -> 0 ->
                               'details') > 1)
SELECT *
FROM d;

