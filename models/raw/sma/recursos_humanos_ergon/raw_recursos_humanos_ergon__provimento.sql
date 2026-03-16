{{
    config(
        alias='provimento',
        materialized="table",
        tags=["raw", "ergon", "provimento"],
        description="Eventos relacionados aos vínculos funcionais tanto com a administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(CAST(NUMFUNC AS STRING), r'\.0$', '') AS int64) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(CAST(NUMVINC AS STRING), r'\.0$', '') AS int64) AS id_vinculo,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DTINI) AS DATE) AS data_inicio,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DTFIM) AS DATE) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(SETOR, r'\.0$', '') AS int64) AS id_setor,
    SAFE_CAST(REGEXP_REPLACE(CAST(CARGO AS STRING), r'\.0$', '') AS int64) AS id_cargo,
    SAFE_CAST(REGEXP_REPLACE(REFERENCIA, r'\.0$', '') AS string) AS id_referencia,
    SAFE_CAST(REGEXP_REPLACE(JORNADA, r'\.0$', '') AS string) AS id_jornada,
    SAFE_CAST(FORMAPROV AS int64) AS id_forma_provimento,
    SAFE_CAST(OBS AS STRING) AS observacoes,
    SAFE_CAST(FLEX_CAMPO_03 AS STRING) AS regime_horas,
    SAFE_CAST(CAST(EMP_CODIGO AS STRING) AS int64) AS id_empresa,
    _airbyte_extracted_at as updated_at
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_PROVIMENTOS_EV')}}
