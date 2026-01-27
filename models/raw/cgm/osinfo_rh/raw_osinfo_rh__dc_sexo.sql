{{
    config(
      alias="sexo",
      description="Sexos definidos para o cadastro de pessoas conforme padrões de sistemas de recursos humanos."
    )
}}

select
    safe_cast(`SEX_CD_SEXO` as integer) as sexo_codigo,
    safe_cast(`SEX_DS_DESCRICAO` as string) as sexo_descricao,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_sexo') }}
