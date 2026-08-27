{{
    config(
        alias='cpf_rio',
        materialized='table',
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {
                "start": 0,
                "end": 100000000000,
                "interval": 26000000,
            },
        },
        cluster_by=["cpf", "situacao_cadastral_tipo", "nome", "nascimento_data"],
    )
}}

select *
from {{ ref("mart_api_bcadastro__cpf") }}
where lower(uf) = 'rj' and id_municipio = '6001'
