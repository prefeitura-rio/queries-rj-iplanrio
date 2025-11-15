{{
    config(
      alias="folhas_emp",
      description="Tabela de FOLHAS_EMP"
    )
}}

select
    safe_cast(NUMERO as integer) as numero,
    safe_cast(MES_ANO as string) as mes_ano,
    safe_cast(TIPO_FOLHA as string) as tipo_folha,
    safe_cast(EMP_CODIGO as integer) as emp_codigo,
    safe_cast(DATA_CONSOL as datetime) as data_consol,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
  FROM {{ source('brutos_contracheque_staging', 'folhas_emp') }}
