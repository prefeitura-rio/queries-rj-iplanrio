{{
    config(
      schema="brutos_sisbicho",
      alias="responsavel_tecnico",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Responsaveis Técnicos do Sistema SisBicho"
    )
}}

select 
    safe_cast(usr_codigo as integer) as codigo_usuario,
    safe_cast(crmv as string) as crm
from {{ source('brutos_sisbicho_staging', 'ResponsavelTecnico') }} 