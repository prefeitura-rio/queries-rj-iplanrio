SELECT
    SAFE_CAST(REGEXP_REPLACE(id_tipo_mobil, r'\.0$', '') AS	STRING) AS 	id_tipo_mobil,
    SAFE_CAST(desc_tipo_mobil AS STRING) AS descricao_tipo_mobil,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.mobil_tipo`