{{
    config(
      alias="funcionarios",
      materialized="table",
      tags=["raw", "contracheques"],
      description="Tabela de FUNCIONARIOS"
    )
}}

select
    safe_cast(NUMERO as integer) as numero,
    safe_cast(NOME as string) as nome,
    safe_cast(CPF as string) as cpf,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'ERGON.FUNCIONARIOS') }}
