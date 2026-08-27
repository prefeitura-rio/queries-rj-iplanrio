{{
    config(
        alias='cnpj_rio',
        materialized='table',
        partition_by={
            "field": "cnpj_particao",
            "data_type": "int64",
            "range": {
                "start": 0,
                "end": 99999999999999,
                "interval": 26000000000,
            },
        },
        cluster_by=["cnpj", "cnae_fiscal", "razao_social", "inicio_atividade_data"],
    )
}}

select *
from {{ ref("mart_api_bcadastro__cnpj") }}
where lower(uf) = 'rj' and id_municipio = '6001'
