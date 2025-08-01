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
    safe_cast(crmv as string) as crm,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
from {{ source('brutos_sisbicho_staging', 'ResponsavelTecnico') }} 