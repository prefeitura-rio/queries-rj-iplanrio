{{
    config(
        alias='unidade',
        description="Unidade de Consumo"
    )
}}
 

SELECT
    SAFE_CAST(ds_unidade AS STRING) AS descricao_unidade,
    SAFE_CAST(ID_unidade AS STRING) AS id_unidade
from {{ source('brutos_compras_materiais_servicos_sigma_staging', 'unidade')}}