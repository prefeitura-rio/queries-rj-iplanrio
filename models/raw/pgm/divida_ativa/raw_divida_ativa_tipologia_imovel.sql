{{
    config(
        schema="brutos_divida_ativa",
        alias="tipologia_imovel",
        materialized="table",
        tags=["raw", "divida_ativa", "tipologia", "imovel"],
        description="Tabela que guarda as descrições das possíveis tipologias dos imóveis vinculados às certidões de dívida ativa (CDA)."
    )
}}

select safe_cast(codTipologia as int64) as id_tipologia_imovel,
       safe_cast(descTipologia as string) as nome_tipologia_imovel,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'TipologiaImovel') }}