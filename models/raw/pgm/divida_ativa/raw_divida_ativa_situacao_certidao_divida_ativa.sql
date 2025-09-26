{{
    config(
        schema="brutos_divida_ativa",
        alias="situacao_certidao_divida_ativa",
        materialized="table",
        tags=["raw", "divida_ativa", "divida", "ativa", "situacao"],
        description="Tabela que descreve os possíveis status de uma certidão de dívida ativa (CDA)."
    )
}}

select safe_cast(codSituacaoCDA as int64) as id_situacao_cda,
       safe_cast(nomSituacaoCDA as string) as descricao_situacao_cda,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'SituacaoCDA') }}