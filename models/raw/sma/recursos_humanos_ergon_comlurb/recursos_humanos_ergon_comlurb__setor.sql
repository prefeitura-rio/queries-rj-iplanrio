{{
    config( alias='setor',schema='recursos_humanos_ergon_comlurb')
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(setor.setor), r'\.0$', '') AS STRING) AS id_setor,
    SAFE_CAST(DATE(dtini) AS DATE) AS data_inicio,
    SAFE_CAST(DATE(dtfim) AS DATE) AS data_fim,
    SAFE_CAST(TRIM(tiposetor) AS STRING) AS tipo,
    SAFE_CAST(TRIM(tipoacesso) AS STRING) AS tipo_acesso,
    SAFE_CAST(TRIM(cep) AS STRING) AS cep,
    SAFE_CAST(REGEXP_REPLACE(TRIM(codlog), r'\.0$', '') AS STRING) AS id_logradouro,
    SAFE_CAST(REGEXP_REPLACE(TRIM(flex_campo_02), r'\.0$', '') AS STRING) AS id_setor_sici,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa_setor,
FROM {{ source('recursos_humanos_ergon_comlurb_staging', 'setor') }}