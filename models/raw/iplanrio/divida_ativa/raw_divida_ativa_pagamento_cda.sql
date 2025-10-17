{{
    config(
        alias="pagamento_cda",
        materialized="table",
        tags=["raw", "divida_ativa", "tipo_pagamento", "pagamento", "guia"],
        description="Tabela que contém o histórico de pagamento de cada cota de cada guia de pagamento vinculada a uma certidão de dívida ativa - CDA. Uma guia de pagamento pode conter mais de uma cota e cada cota pode estar vinculada a mais de uma CDA."
    )
}}

select safe_cast(IDPagamentosCDA as int64) as id_pagamento_cda,
  safe_cast(NumCDA as int64) as id_certidao_divida_ativa,
  safe_cast(numGuiaPagamento as int64) as id_guia_pagamento,
  safe_cast(numCotaPagamento as int64) as id_cota_guia_pagamento,
  safe_cast(ObjPago as string) as codigo_objeto_pago,
  case safe_cast(ObjPago as string)
    when 'P' then 'Valor principal'
    when 'H' then 'Honorários'
    when 'G' then 'GRERJ'
    else 'Não classificado'
  end as nome_objeto_pago,
  safe_cast(codTipoPagamento as int64) as codigo_tipo_pagamento,
  ifnull(b.nome_tipo_pagamento, 'Não identificado') as nome_tipo_pagamento,
  safe_cast(ValTotalPG as numeric) as valor_total_pago,
  safe_cast(ValJurosGuiaPG as numeric) as valor_juros_guia_pago,
  a._prefect_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging_prefect', 'PagamentosCDA') }} a
left join {{ ref('raw_divida_ativa_tipo_pagamento_guia') }} b on b.id_tipo_pagamento = cast(a.codTipoPagamento as int64)
