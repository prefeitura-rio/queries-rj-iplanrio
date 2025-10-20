{{
    config(
        alias="cota_pagamento_associada",
        materialized="table",
        tags=["raw", "divida_ativa", "cota_pagamento", "cota", "pagamento"],
        description="Tabela que contém os registros das cotas de parcelamento das guias de pagamento que foram substituídas por outras cotas novas. A substituição de cotas ocorre principalmente quando há atraso de pagamento e uma nova guia é gerada para impedir o cancelamento do parcelamento."
    )
}}

select safe_cast(a.numGuiaPagamento as int64) as id_guia_pagamento,
  safe_cast(a.numCotaPagamento as int64) as id_cota_guia_pagamento,
  safe_cast(a.numGuiaPagamentoAssoc as int64) as id_guia_pagamento_associada,
  safe_cast(a.numCotaPagamentoAssoc as int64) as id_cota_guia_pagamento_associada,
  a._prefect_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging_prefect', 'Cotas_Associadas') }} a