{{
    config( alias='setor_h',schema='recursos_humanos_ergon_comlurb')
}}
SELECT
    SAFE_CAST(DATE(dataini) AS DATE) AS data_inicio,
    SAFE_CAST(REGEXP_REPLACE(TRIM(setor), r'\.0$', '') AS STRING) AS id_setor_sici,
    SAFE_CAST(TRIM(nomesetor) AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(TRIM(paisetor), r'\.0$', '') AS STRING) AS id_setor_pai_sici,
    SAFE_CAST(DATE(datafim) AS DATE) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa_sici,
    SAFE_CAST(TRIM(flex_campo_01) AS STRING) AS sigla,
    SAFE_CAST(REGEXP_REPLACE(TRIM(flex_campo_02), r'\.0$', '') AS STRING) AS id_empresa_prevrio,
    SAFE_CAST(REGEXP_REPLACE(TRIM(flex_campo_05), r'\.0$', '') AS STRING) AS id_secretaria_sici,
    SAFE_CAST(TRIM(extinto) AS STRING) AS extinto,
    SAFE_CAST(TRIM(nomesetorlongo) AS STRING) AS nome_completo,
FROM rj-smfp.recursos_humanos_ergon_comlurb_staging.setor_h AS t