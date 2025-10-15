{{
    config(
        schema="brutos_divida_ativa",
        alias="tipo_pagamento_guia",
        materialized="table",
        tags=["raw", "divida_ativa", "tipo_pagamento", "pagamento", "guia"],
        description="Tabela que descreve os tipos de pagamentos possíveis para uma Guia de Pagamento, como Parcelamento, Liquidação, Antecipação, Regularização, entre outros."
    )
}}

select safe_cast(NumValor as int64) as id_tipo_pagamento,
  safe_cast(Descricao as string) as nome_tipo_pagamento,
  _airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'Enumerator') }}
where DesGrupo like '%TiposPagamento%'