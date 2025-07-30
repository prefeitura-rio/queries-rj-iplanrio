


{{
    config(
      schema="brutos_sisbicho",
      alias="porte",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de portes de animais"
    )
}}

select
    safe_cast(idporte as integer) as id_porte,
    safe_cast(porte.porte as string) as porte_nome,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Porte') }} 