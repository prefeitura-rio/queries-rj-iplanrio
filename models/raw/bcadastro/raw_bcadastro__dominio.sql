{{
    config(
        alias="dominio",
        schema="brutos_bcadastro",
        materialized="table",
    )
}}

select id, {{ proper_br("descricao") }} as descricao, column, 'cpf' as source
from {{ source("brutos_bcadastro_staging","dominio_cpf") }}
union all
select id, {{ proper_br("descricao") }} as descricao, column, 'cnpj' as source
from {{ source("brutos_bcadastro_staging","dominio_cnpj") }}
