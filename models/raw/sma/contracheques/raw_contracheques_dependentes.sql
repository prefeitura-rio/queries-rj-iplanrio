{{
    config(
      alias="dependentes",
      description="Tabela de DEPENDENTES"
    )
}}

select
    safe_cast(NUMERO as integer) as numero,
    safe_cast(NUMFUNC as integer) as numfunc,
    safe_cast(CPF as string) as cpf,
    safe_cast(NOME as string) as nome,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'dependentes') }}
