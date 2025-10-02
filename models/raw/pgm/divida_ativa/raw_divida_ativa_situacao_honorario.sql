{{
    config(
        schema="brutos_divida_ativa",
        alias="situacao_honorario",
        materialized="table",
        tags=["raw", "divida_ativa", "tipo_honorario", "honorário"],
        description="Tabela que descreve as possíveis situações de cobranças de honorários referentes ao processo de cálculo das certidões de dívida ativa (CDA)"
    )
}}

select safe_cast(numSituacao as int64) as id_situacao_honorario,
       safe_cast(DesSituacao as string) as nome_situacao_honorario,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'Situacao_honorarios') }}