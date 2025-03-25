SELECT
    SAFE_CAST(REGEXP_REPLACE(id_orgao_usu, r'\.0$', '') AS	STRING) AS 	id_orgao,
    SAFE_CAST(nm_orgao_usu	AS	STRING) AS 	nome_orgao,
    SAFE_CAST(sigla_orgao_usu	AS	STRING) AS 	sigla_orgao,
    SAFE_CAST(REGEXP_REPLACE(cod_orgao_usu, r'\.0$', '') AS	STRING) AS 	id_orgao_sici,
    SAFE_CAST(REGEXP_REPLACE(is_externo_orgao_usu, r'\.0$', '')	AS	INT64) AS is_orgao_externo,
    SAFE_CAST(REGEXP_REPLACE(his_ativo, r'\.0$', '')	AS	INT64) AS is_orgao_ativo,
    SAFE_CAST(REGEXP_REPLACE(is_peticionamento, r'\.0$', '')	AS	INT64) AS is_peticionamento,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.orgao_usuario`