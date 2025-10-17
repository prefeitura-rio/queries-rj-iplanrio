{{
    config(
        alias="situacao_guia_pagamento",
        materialized="table",
        tags=["raw", "divida_ativa", "situacao_guia", "guia", "pagamento"],
        description="Tabela que descreve as possíveis situações das guias de pagamento calculadas. Uma guia de pagamento pode estar vinculada a mais de uma CDA."
    )
}}

select safe_cast(idSituacaoGuia as int64) as id_situacao_guia_pagamento,
       safe_cast(descSituacaoGuia as string) as nome_situacao_guia_pagamento,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'SituacaoGuia') }}