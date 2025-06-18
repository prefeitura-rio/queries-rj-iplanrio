{{
    config(
      schema="brutos_sisbicho",
      alias="usuario",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Usuários do Sistema SisBicho"
    )
}}

SELECT 
    SAFE_cast([USR_CODIGO] as string) as usuario_codigo,
    SAFE_cast([CRMV] as string) as crmv_numero
FROM {{ source('brutos_sisbicho_staging', 'FR_USUARIO') }} 