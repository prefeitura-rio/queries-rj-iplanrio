SELECT
    SAFE_CAST(REGEXP_REPLACE(id_classificacao, r'\.0$', '') AS	STRING) AS 	id_classificacao,
    SAFE_CAST(descr_classificacao AS STRING) AS descricao_classificacao,
    SAFE_CAST(REGEXP_REPLACE(his_ativo, r'\.0$', '')	AS	INT64) AS is_ativo,
    SAFE_CAST(codificacao AS STRING) AS codificacao,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.classificacao`