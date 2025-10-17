{{
    config(
        alias="tipo_receita",
        materialized="table",
        tags=["raw", "divida_ativa", "divida", "receita"],
        description="Tabela que descreve os possíveis códigos de receita que são usados para classificar as Guias de Pagamento."
    )
}}

select safe_cast(codReceita as string) as codigo_receita,
       safe_cast(descReceita as string) as nome_receita,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'Receita') }}