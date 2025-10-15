{{
    config(
        schema="brutos_divida_ativa",
        alias="tipologia_imovel",
        materialized="table",
        tags=["raw", "divida_ativa", "tipologia", "imovel"],
        description="Tabela que descreve os possíveis tipologias de imóveis que estejam vinculados com certidões de dívida ativa (CDA), para os casos de dívidas de IPTU ou Mais Valia."
    )
}}

select safe_cast(codTipologia as int64) as id_tipologia_imovel,
       safe_cast(descTipologia as string) as nome_tipologia_imovel,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'TipologiaImovel') }}