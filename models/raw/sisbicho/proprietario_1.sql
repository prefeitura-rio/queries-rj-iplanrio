{{
    config(
      alias="animal_proprietario_1",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de vínculos entre animais e proprietários"
    )
}}

with proprietario as (
    select * from {{ ref("raw_sisbicho_animal_proprietario") }}
)

select * from proprietario
limit 100
