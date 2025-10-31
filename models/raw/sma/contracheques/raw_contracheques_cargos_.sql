{{
    config(
      alias="cargos_",
      description="Tabela de CARGOS_"
    )
}}

select
    safe_cast(CARGO as integer) as cargo,
    safe_cast(NOME as string) as nome,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'cargos_') }}
