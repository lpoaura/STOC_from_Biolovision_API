/* TODO peuplement des relevés STOC EPS */
UPDATE import_vn.forms_json
SET
    site=site
    WHERE
        item #>> '{protocol,protocol_name}' LIKE 'STOC_EPS'
;

/* TODO Peuplement des observations détaillées STOC EPS */
UPDATE import_vn.observations_json
SET
    site=site
    WHERE
--         item #>> '{observers,0,protocol,protocol_name}' LIKE 'STOC_EPS'
id_form_universal IN (SELECT source_id_universal FROM pr_stoc.t_releves WHERE source_bdd LIKE 'vn%')
;

SELECT
    split_part(o.item #>> '{observers,0,id_universal}', '_', 1) AS       source
  , count(DISTINCT f.*)                                         AS       nbform
  , count(o.*)                                                  AS       nbobs
  , min((f.item #>> '{date_start}')::DATE)                      AS       min_date_start
  , max((f.item #>> '{date_start}')::DATE)                      AS       min_date_start
--   , o.item #>> '{observers,0,protocol,protocol_name}' LIKE 'STOC_EPS' AS obs_contains_protocol_eps_data
--   , (array_agg(replace(url_source, '&id=', '&uid=') || (o.item #>> '{observers,0,id_universal}'))
  , (array_agg('https://www.faune-france.org/index.php?m_id=54&uid=' || (o.item #>> '{observers,0,id_universal}'))
     FILTER (WHERE o.item #> '{observers,0}' ? 'protocol'))[:10]         obs_with_protocol_data
--   , (array_agg(replace(url_source, '&id=', '&uid=') || (o.item #>> '{observers,0,id_universal}'))
  , (array_agg('https://www.faune-france.org/index.php?m_id=54&uid=' || (o.item #>> '{observers,0,id_universal}'))
     FILTER (WHERE NOT o.item #> '{observers,0}' ? 'protocol'))[:10]     obs_without_protocol_data
--   , count(o.*) FILTER (WHERE o.item #>> '{observers,0,protocol,protocol_name}' LIKE 'STOC_EPS')     o_is_eps
--   , count(o.*) FILTER (WHERE o.item #>> '{observers,0,protocol,protocol_name}' NOT LIKE 'STOC_EPS') o_isnot_eps
  , count(o.*) FILTER (WHERE o.item #> '{observers,0}' ? 'protocol')     has_protocol
  , count(o.*) FILTER (WHERE NOT o.item #> '{observers,0}' ? 'protocol') hasno_protocol
--   , array_agg(DISTINCT o.item #>> '{observers,0,protocol,protocol_name}') protocols
    FROM
        import_vn.forms_json f
            JOIN gn_synthese.t_sources s ON f.site = s.name_source
            LEFT JOIN import_vn.observations_json o ON o.id_form_universal = f.item ->> 'id_form_universal'
    WHERE
          f.item #>> '{protocol,protocol_name}' LIKE 'STOC_EPS'
      AND (f.item #>> '{date_start}')::DATE > '2021-01-01'
    GROUP BY
        split_part(o.item #>> '{observers,0,id_universal}', '_', 1)
--       , o.item #>> '{observers,0,protocol,protocol_name}' LIKE 'STOC_EPS'

;

WITH
    t1 AS (
        SELECT
            f.*
            FROM
                import_vn.forms_json f
                    LEFT JOIN import_vn.observations_json o ON o.id_form_universal = f.item ->> 'id_form_universal'
            WHERE
                  f.item #>> '{protocol,protocol_name}' LIKE 'STOC_EPS'
              AND NOT o.item #> '{observers,0}' ? 'protocol'
    )
SELECT *
    FROM
        import_vn.observations_json
            JOIN t1 ON t1.item ->> 'id_form_universal' = id_form_universal
;

WITH
    t1 AS (
        SELECT
            detail.obj                               AS detail
          , obs.id
          , obs.site
          , obs.item #> '{observers,0,id_universal}' AS source_id_universal
          , obs.id_form_universal
          , to_timestamp(obs.update_ts)
            FROM
                import_vn.observations_json obs
                    LEFT JOIN LATERAL jsonb_array_elements(obs.item #> '{observers,0,details}') AS detail(obj) ON TRUE
            WHERE
                item #>> '{observers,0,protocol,protocol_name}' LIKE 'STOC_EPS')
SELECT
    count(*)
  , count(DISTINCT source_id_universal)
  , count(DISTINCT id_form_universal)
    FROM
        t1
;

SELECT
    jsonb_pretty(item)
    FROM
        import_vn.forms_json
    WHERE
        item #>> '{protocol,protocol_name}' LIKE 'STOC_EPS'
    LIMIT 5;


