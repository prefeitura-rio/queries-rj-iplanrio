{{
    config(
      alias="status",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") ,
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Status de Animais do Sistema SisBicho"
    )
}}

select
    safe_cast(IDStatus AS int64) AS id_status,
    safe_cast(Status.Status AS string) AS status_nome,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Status') }}
