SELECT
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",data_ini_lot) AS datetime) AS data_inicio_lotacao,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",data_fim_lot) AS datetime) AS	data_fim_lotacao,
    SAFE_CAST(REGEXP_REPLACE(id_orgao_usu, r'\.0$', '') AS	STRING) AS 	id_orgao_sici,
    SAFE_CAST(REGEXP_REPLACE(id_lotacao_pai, r'\.0$', '') AS STRING) AS id_lotacao_pai,
    SAFE_CAST(REGEXP_REPLACE(id_lotacao, r'\.0$', '') AS	STRING) AS 	id_lotacao,
    SAFE_CAST(REGEXP_REPLACE(sigla_lotacao, r'\.0$', '') AS	STRING) AS 	id_lotacao_sici,
    SAFE_CAST(nome_lotacao	AS	STRING) AS nome_lotacao,
    SAFE_CAST(REGEXP_REPLACE(is_externa_lotacao, r'\.0$', '')	AS	INT64) AS 	is_lotacao_externa
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.lotacao`
