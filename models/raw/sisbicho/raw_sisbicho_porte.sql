{{
    config(
      schema="brutos_sisbicho",
      alias="porte",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de portes de animais"
    )
}}

select
    safe_cast(idporte as integer) as id_porte,
    safe_cast(porte.porte as string) as porte_nome
FROM {{ source('brutos_sisbicho_staging', 'Porte') }} 