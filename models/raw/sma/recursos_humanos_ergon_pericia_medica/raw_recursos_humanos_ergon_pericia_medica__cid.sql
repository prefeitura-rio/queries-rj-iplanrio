{{
    config(
        alias='cid'
    )
}}

SELECT safe_cast(TIPO AS STRING) AS tipo, 
safe_cast(NIVEL AS STRING) AS nivel,
safe_cast(REFER AS STRING) AS refer,
safe_cast(CODIGO AS STRING) AS codigo,
safe_cast(CLASSIF AS STRING) AS classif,
safe_cast(COD_PAI AS STRING) AS cod_pai,
safe_cast(CAPITULO AS STRING) AS capitulo,
safe_cast(DESCRICAO AS STRING) AS descricao,
safe_cast(EXCLUIDOS AS STRING) AS excluidos,
safe_cast(GUID_GRUPO AS STRING) AS guid_grupo,
safe_cast(RESTR_SEXO AS STRING) AS restr_sexo,
safe_cast(CAUSA_OBITO AS STRING) AS causa_obito,
safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01,
safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02,
safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03,
safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04,
safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05,
safe_cast(FLEX_CAMPO_06 AS STRING) AS flex_campo_06,
safe_cast(FLEX_CAMPO_07 AS STRING) AS flex_campo_07,
safe_cast(FLEX_CAMPO_08 AS STRING) AS flex_campo_08,
safe_cast(FLEX_CAMPO_09 AS STRING) AS flex_campo_09,
safe_cast(FLEX_CAMPO_10 AS STRING) AS flex_campo_10,
safe_cast(FLEX_CAMPO_11 AS STRING) AS flex_campo_11,
safe_cast(FLEX_CAMPO_12 AS STRING) AS flex_campo_12,
safe_cast(FLEX_CAMPO_13 AS STRING) AS flex_campo_13,
safe_cast(FLEX_CAMPO_14 AS STRING) AS flex_campo_14,
safe_cast(FLEX_CAMPO_15 AS STRING) AS flex_campo_15,
safe_cast(DESCRICAO_ABREV AS STRING) AS descricao_abrev,
_airbyte_extracted_at as datalake_loaded_at, 
current_timestamp() as datalake_transformed_at 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'CID') }}

