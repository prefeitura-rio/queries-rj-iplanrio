{{
    config(
      alias="sexo",
      description="Sexos definidos para o cadastro de pessoas conforme padrões de sistemas de recursos humanos.",
      materialized='view'
    )
}}

select
    safe_cast(`SEX_CD_SEXO` as int64) as sexo_codigo,
    safe_cast(`SEX_DS_DESCRICAO` as string) as sexo_descricao,
    DATETIME(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    DATETIME(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_sexo') }}
