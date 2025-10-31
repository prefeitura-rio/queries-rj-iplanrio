{{
    config(
      alias="dependencias",
      materialized="table",
      tags=["raw", "contracheques"],
      description="Tabela de DEPENDENCIAS"
    )
}}

select
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(TIPODEPEN as integer) as tipodepen,
    safe_cast(DTINI as datetime) as dtini,
    safe_cast(DTFIM as datetime) as dtfim,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'ERGON.DEPENDENCIAS') }}
