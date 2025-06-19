{{
    config(
      schema="brutos_sisbicho",
      alias="porte",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de portes de animais"
    )
}}

SELECT 
    SAFE_cast(IDPorte as integer) as id_porte,
    SAFE_cast(Porte.Porte as string) as porte_nome
FROM {{ source('brutos_sisbicho_staging', 'Porte') }} 