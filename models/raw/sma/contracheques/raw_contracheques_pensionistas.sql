{{
    config(
      alias="pensionistas",
      description="Tabela de PENSIONISTAS"
    )
}}

select
    safe_cast(NUMERO as integer) as numero,
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(NUMVINC as integer) as numvinc,
    safe_cast(EMP_CODIGO as integer) as emp_codigo,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'PENSIONISTAS') }}
