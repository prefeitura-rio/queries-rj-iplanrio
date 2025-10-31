{{
    config(
      alias="vinculos",
      description="Tabela de VINCULOS"
    )
}}

select
    safe_cast(NUMERO as integer) as numero,
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(EMP_CODIGO as integer) as emp_codigo,
    safe_cast(MATRIC as integer) as matric,
    safe_cast(DTEXERC as datetime) as dtexerc,
    safe_cast(DTAPOSENT as datetime) as dtaposent,
    safe_cast(REGIMEJUR as string) as regimejur,
    safe_cast(DTVAC as datetime) as dtvac,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'vinculos') }}
