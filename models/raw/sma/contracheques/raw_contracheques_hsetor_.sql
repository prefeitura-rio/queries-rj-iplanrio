{{
    config(
      alias="hsetor_",
      description="Tabela de HSETOR_"
    )
}}

select
    safe_cast(SETOR as integer) as setor,
    safe_cast(EMP_CODIGO as integer) as emp_codigo,
    safe_cast(DATAINI as datetime) as dataini,
    safe_cast(DATAFIM as datetime) as datafim,
    safe_cast(FLEX_CAMPO_01 as string) as flex_campo_01,
    safe_cast(FLEX_CAMPO_05 as string) as flex_campo_05,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'HSETOR_') }}
