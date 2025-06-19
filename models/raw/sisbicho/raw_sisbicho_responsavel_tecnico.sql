{{
    config(
      schema="brutos_sisbicho",
      alias="responsavel_tecnico",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Responsaveis Técnicos do Sistema SisBicho"
    )
}}

SELECT 
    SAFE_cast(USR_CODIGO as integer) as usr_codigo,
    SAFE_cast(CRMV as String) as crmv
FROM {{ source('brutos_sisbicho_staging', 'responsavel_tecnico') }} 