{{
    config(
        alias='cargo',
        materialized="table",
        tags=["raw", "ergon", "cargo"],
        description="Tabela que contém os registros dos cargos para os quais os funcionários são nomeados em seus provimentos."
    )
}}

SELECT
    {{ clean_and_cast('CARGO', 'int64') }} AS id_cargo,
    SAFE_CAST(NOME AS STRING) AS nome,
    SAFE_CAST(CATEGORIA AS STRING) AS categoria,
    SAFE_CAST(SUBCATEGORIA AS STRING) AS subcategoria,
    SAFE_CAST(CONTROLE_VAGA AS STRING) AS tipo_controle_vaga,
    {{ clean_and_cast('ESCOLARIDADE', 'string') }} AS escolaridade,
    SAFE_CAST(E_AGLUTINADOR AS STRING) AS aglutinador,
    SAFE_CAST(TIPO_CARGO AS STRING) AS tipo_cargo,
    SAFE_CAST( SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DT_EXTINCAO) AS DATE) AS dt_extincao,
    SAFE_CAST(CARGO_FUNCAO AS STRING) AS cargo_funcao,
    _airbyte_extracted_at AS updated_at
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_CARGOS_')}}