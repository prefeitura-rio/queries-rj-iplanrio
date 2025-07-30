{{
    config(
      schema="brutos_sisbicho",
      alias="status",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Status de Animais do Sistema SisBicho"
    )
}}

select
    safe_cast(IDStatus AS int64) AS id_status,
    safe_cast(Status.Status AS string) AS status_nome,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Status') }}
