{{
    config(
      schema="brutos_sisbicho",
      alias="status",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Status de Animais do Sistema SisBicho"
    )
}}

SELECT 
    SAFE_CAST(IDStatus AS INT64) AS id_status,
    SAFE_CAST(Status.Status AS STRING) AS status_nome
FROM {{ source('brutos_sisbicho_staging', 'Status') }}
