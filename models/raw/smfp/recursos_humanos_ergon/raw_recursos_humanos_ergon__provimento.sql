{{
    config(
        alias='provimento',
        schema='recursos_humanos_ergon'
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(CAST(NUMFUNC AS STRING), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(CAST(NUMVINC AS STRING), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DTINI) AS DATE) AS data_inicio,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DTFIM) AS DATE) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(SETOR, r'\.0$', '') AS STRING) AS id_setor,
    SAFE_CAST(REGEXP_REPLACE(CAST(CARGO AS STRING), r'\.0$', '') AS STRING) AS id_cargo,
    SAFE_CAST(REGEXP_REPLACE(REFERENCIA, r'\.0$', '') AS STRING) AS id_referencia,
    SAFE_CAST(REGEXP_REPLACE(JORNADA, r'\.0$', '') AS STRING) AS id_jornada,
    SAFE_CAST(FORMAPROV AS STRING) AS forma,
    SAFE_CAST(OBS AS STRING) AS observacoes,
    SAFE_CAST(FLEX_CAMPO_03 AS STRING) AS regime_horas,
    SAFE_CAST(CAST(EMP_CODIGO AS STRING) AS STRING) AS empresa_vinculo,
    -- Campos novos que não estavam na query antiga
    SAFE_CAST(CAST(PONTLEI AS STRING) AS STRING) AS ponto_lei,
    SAFE_CAST(CAST(PONTPUBL AS STRING) AS STRING) AS ponto_publicacao,
    SAFE_CAST(CAST(HORARIOTRAB AS STRING) AS STRING) AS horario_trabalho,
    SAFE_CAST(CAST(NUMERO_VAGA AS STRING) AS STRING) AS numero_vaga,
    SAFE_CAST(FLEX_CAMPO_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(FLEX_CAMPO_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(FLEX_CAMPO_20 AS STRING) AS flex_campo_20,
    _airbyte_extracted_at as updated_at
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_PROVIMENTOS_EV')}}
