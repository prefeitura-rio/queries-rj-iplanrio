{{
    config(
        alias="nulls_consolidado",
        schema='debug_bcadastro',
        materialized="table",
    )
}}

select 'cpf' as table, *
from {{ ref('int_bcadastro_nulls_cpf') }}
union all
select 'cnpj' as table, *
from {{ ref('int_bcadastro_nulls_cnpj') }}
union all
select 'cno' as table, *
from {{ ref('int_bcadastro_nulls_cno') }}
union all
select 'caepf' as table, *
from {{ ref('int_bcadastro_nulls_caepf') }}





