


{{
    config(
      alias="porte",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") ,
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de portes de animais"
    )
}}

select
    safe_cast(idporte as integer) as id_porte,
    safe_cast(porte.porte as string) as porte_nome,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Porte') }} 