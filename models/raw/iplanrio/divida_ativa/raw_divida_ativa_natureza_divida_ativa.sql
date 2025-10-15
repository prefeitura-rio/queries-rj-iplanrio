{{
    config(
        schema="brutos_divida_ativa",
        alias="natureza_divida_ativa",
        materialized="table",
        tags=["raw", "divida_ativa", "divida", "ativa", "natureza", "natureza_divida"],
        description="Tabela que descreve os possíveis tipos de natureza de dívida das certidões de dívida ativa (CDA)."
    )
}}

select safe_cast(idNaturezaDivida as int64) as id_natureza_divida,
       safe_cast(nomNaturezaDivida as string) as nome_natureza_divida,
       safe_cast(descNaturezaDivida as string) as descricao_natureza_divida,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'NaturezaDivida') }}