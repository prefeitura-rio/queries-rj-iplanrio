{{
    config(
      alias="rubricas",
      description="Tabela de RUBRICAS"
    )
}}

select
    safe_cast(RUBRICA as integer) as rubrica,
    safe_cast(TIPORUBR as string) as tiporubr,
    safe_cast(NOME_ABREV as string) as nome_abrev,
    safe_cast(NOME as string) as nome,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'rubricas') }}
