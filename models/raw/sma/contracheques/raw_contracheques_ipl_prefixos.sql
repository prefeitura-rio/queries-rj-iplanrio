{{
    config(
      alias="ipl_prefixos",
      description="Tabela de IPL_PREFIXOS"
    )
}}

select
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(NUMVINC as integer) as numvinc,
    safe_cast(MESANO as string) as mesano,
    safe_cast(PREFIXO as integer) as prefixo,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'IPL_PREFIXOS') }}
