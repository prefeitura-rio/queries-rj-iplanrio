{{
    config(
        schema="brutos_divida_ativa",
        alias="tipo_pagamento_guia",
        materialized="table",
        tags=["raw", "divida_ativa", "tipo_pagamento", "pagamento", "guia"],
        description="Tabela que descreve as possíveis situações das guias de pagamento calculadas. Uma guia de pagamento pode estar vinculada a mais de uma CDA."
    )
}}

select safe_cast(codTipoPagamento as int64) as id_tipo_pagamento_guia,
       safe_cast(descTextoAmigavel as string) as texto_amigavel,
       safe_cast(descTextoJudicial as string) as texto_judicial,
       safe_cast(descTextoTodasParcelasAmigavel as string) as texto_amigavel_todas_parcelas,
       safe_cast(descTextoTodasParcelasJudicial as string) as texto_judicial_todas_parcelas,
       safe_cast(descTextoPrimeirasGuiasAmigavel as string) as texto_amigavel_primeiras_guias,
       safe_cast(descTextoPrimeirasGuiasJudicial as string) as texto_judicial_primeiras_guias,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'TextoGuia') }}