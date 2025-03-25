SELECT
    SAFE_CAST(REGEXP_REPLACE(id_mod, r'\.0$', '') AS	STRING) AS 	id_modelo,
    SAFE_CAST(nm_mod	AS	STRING) AS 	nome_modelo,
    SAFE_CAST(desc_mod	AS	STRING) AS descricao_modelo,
    SAFE_CAST( REGEXP_REPLACE(his_id_ini, r'\.0$', '') AS	STRING)	AS id_modelo_original,
    -- TODO: check this 2 columns bellow his_idc_ini, his_idc_fim
    SAFE_CAST(REGEXP_REPLACE(his_idc_ini, r'\.0$', '')	AS	STRING)	AS his_idc_ini,
    SAFE_CAST(REGEXP_REPLACE(his_idc_fim, r'\.0$', '')	AS	STRING)	AS his_idc_fim,
    SAFE_CAST(REGEXP_REPLACE(his_ativo, r'\.0$', '')	AS	INT64) AS is_ativo,
    SAFE_CAST(REGEXP_REPLACE(is_peticionamento, r'\.0$', '')	AS	INT64) AS is_peticionamento,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.modelo`