{{
    config(
      alias="fatores_rubrica",
      description="Tabela de FATORES_RUBRICA"
    )
}}

select
    safe_cast(FATOR as integer) as fator,
    safe_cast(RUBRICA as integer) as rubrica,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'fatores_rubricas') }}
