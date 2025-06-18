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
    SAFE_cast([IDStatus] as integer) as id_status,
    SAFE_cast([Status] as string) as status_nome
FROM {{ source('brutos_sisbicho_staging', 'Status') }} 