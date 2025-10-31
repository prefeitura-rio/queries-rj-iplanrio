{{
    config(
      alias="fichas_vinculos",
      description="Tabela de FICHAS_VINCULOS"
    )
}}

select
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(NUMVINC as integer) as numvinc,
    safe_cast(MES_ANO_FOLHA as string) as mes_ano_folha,
    safe_cast(NUM_FOLHA as integer) as num_folha,
    safe_cast(EMP_CODIGO as integer) as emp_codigo,
    safe_cast(FICHA as integer) as ficha,
    safe_cast(NUMPENS as integer) as numpens,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'FICHAS_VINCULOS') }}
