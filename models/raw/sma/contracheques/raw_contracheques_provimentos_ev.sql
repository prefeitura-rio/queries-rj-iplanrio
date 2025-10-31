{{
    config(
      alias="provimentos_ev",
      materialized="table",
      tags=["raw", "contracheques"],
      description="Tabela de PROVIMENTOS_EV"
    )
}}

select
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(NUMVINC as integer) as numvinc,
    safe_cast(DTINI as datetime) as dtini,
    safe_cast(DTFIM as datetime) as dtfim,
    safe_cast(CARGO as integer) as cargo,
    safe_cast(SETOR as integer) as setor,
    safe_cast(JORNADA as string) as jornada,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'ERGON.PROVIMENTOS_EV') }}
