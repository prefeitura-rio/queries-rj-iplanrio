SELECT
    SAFE_CAST(REGEXP_REPLACE(id_nivel_acesso, r'\.0$', '') AS	STRING) AS 	id_nivel_acesso,
    SAFE_CAST(nm_nivel_acesso AS STRING)AS nome_nivel_acesso
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.nivel_acesso`