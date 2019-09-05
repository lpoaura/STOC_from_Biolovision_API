/* Recherche de formulaires STOC en doublon */
SELECT
    id_releve
  , carre_numnat
  , passage_mnhn
  , date
  , type_eps
  , point_num
  , codesp_euring
  , t_releves.source_bdd
--   , sum(nombre)
FROM
    pr_stoc.t_observations
        JOIN pr_stoc.t_releves ON t_observations.id_releve = t_releves.id
        LEFT JOIN pr_stoc.cor_euring_vn_taxref ON codesp_euring = code_euring AND ref_tax IS TRUE
WHERE
    t_releves.source_bdd LIKE 'vn%' AND
    carre_numnat = 11158 AND
    date BETWEEN '2019-04-20' AND '2019-04-24' AND
    point_num = 1
;

SELECT *
FROM pr_stoc.t_observations
ORDER BY
    update_ts ASC
LIMIT 20;


UPDATE import_vn.observations_json
SET
    site = site
WHERE
        id_form_universal IN (SELECT source_id_universal
                              FROM pr_stoc.t_releves
                              WHERE source_bdd LIKE 'vn%')

SELECT min(id), max(id)
FROM pr_stoc.t_releves
WHERE
    source_bdd LIKE 'vn%';

UPDATE pr_stoc.t_observations
SET
    update_ts = to_timestamp(obsjson.update)
FROM import_vn.observations_json obsjson
WHERE
    source_bdd = site AND
    source_id = obsjson.id;


SELECT form.id, form.item ->> 'comment' as comment, form.item #>> '{protocol, protocol_name}' as pr_name, form.item ->> 'full_form' as full_form, jsonb_pretty(form.item), array_agg(obs.id)
FROM import_vn.forms_json form join import_vn.observations_json obs on form.item ->> 'id_form_universal' = obs.id_form_universal
group by form.id, form.item order by form.id desc;

select jsonb_pretty(item) from import_vn.observations_json where id = 474534 and site like 'vn07';
select jsonb_pretty(item) from import_vn.forms_json where item ->> 'id_form_universal' like '65_354745';