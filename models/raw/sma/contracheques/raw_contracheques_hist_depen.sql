{{
    config(
      alias="hist_depen",
      materialized="table",
      tags=["raw", "contracheques"],
      description="Tabela de HIST_DEPEN"
    )
}}

select
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(INVALIDO as string) as invalido,
    safe_cast(DTINI as datetime) as dtini,
    safe_cast(DTFIM as datetime) as dtfim,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'ERGON.HIST_DEPEN') }}
