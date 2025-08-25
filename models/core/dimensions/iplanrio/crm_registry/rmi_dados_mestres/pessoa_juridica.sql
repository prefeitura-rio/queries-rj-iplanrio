{{
    config(
        alias="pessoa_juridica",
        schema="rmi_crm_dados_mestres",
        materialized="table",
        partition_by={
            "field": "cnpj_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with bcadastro_source as (select * from {{ source("brutos_bcadastro", "cnpj") }})

select *
from bcadastro_source
