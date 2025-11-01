{{
    config(
      alias="fitabanco",
      description="Tabela de FITABANCO"
    )
}}

select
    safe_cast(FICHA as integer) as ficha,
    safe_cast(NUMDEPEN as integer) as numdepen,
    safe_cast(BANCO as integer) as banco,
    safe_cast(AGENCIA as integer) as agencia,
    safe_cast(CONTA as string) as conta,
    safe_cast(SETOR as integer) as setor,
    safe_cast(MES_ANO as string) as mes_ano,
    safe_cast(CARGO as integer) as cargo,
    safe_cast(REFERENCIA as string) as referencia,
    safe_cast(CPF as string) as cpf,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'fitabanco') }}
