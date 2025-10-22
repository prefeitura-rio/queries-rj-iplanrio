{{
    config(
        alias='unidade_servico',
        description="Unidade de Serviço"
    )
}}

SELECT
  SAFE_CAST(NR_UNIDADE AS INT64) AS id_unidade_servico,
  SAFE_CAST(Ds_Unidade_Servico AS STRING) AS descricao_unidade_servico

from {{ source('brutos_compras_materiais_servicos_sigma_staging', 'unidade_servico')}}