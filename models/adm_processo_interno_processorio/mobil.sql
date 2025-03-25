SELECT
    SAFE_CAST(REGEXP_REPLACE(id_mobil, r'\.0$', '') AS	STRING) AS 	id_mobil,
    SAFE_CAST(REGEXP_REPLACE(id_doc, r'\.0$', '') AS	STRING) AS 	id_documento,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.mobil`