{{
    config(
        schema="brutos_divida_ativa",
        alias="cota_pagamento",
        materialized="table",
        tags=["raw", "divida_ativa", "cota_pagamento", "cota", "pagamento"],
        description="Tabela que contém os registros das cotas referentes às parcelas das guias de pagammentos geradas para parcelamento de dívidas. Uma guia de pagamento pode relacionar mais de uma CDA."
    )
}}

select safe_cast(a.numGuiaPagamento as int64) as id_guia_pagamento,
  safe_cast(a.numCotaPagamento as int64) as id_cota_guia_pagamento,
  case 
    when b.numCotaPagamentoAssoc is not null then true
    else false
  end as cota_substituida,
  safe_cast(a.valCotaPagamento as numeric) as valor_cota_guia_pagamento,
  safe_cast(a.dtVencimento as date) as data_vencimento,
  safe_cast(a.stsCotaPagamento as int64) as situacao_pagamento_cota,
  case safe_cast(stsCotaPagamento as int64)
    when 1 then 'Em aberto'
    when 4 then 'Paga'
    else 'Situação não mapeada'
  end as descricao_situacao_pagamento_cota,
  safe_cast(a.valPrincipal as numeric) as valor_principal,
  safe_cast(ValHonorarios as numeric) as valor_honorarios,
  safe_cast(a.ValJuros as numeric) as valor_juros,
  safe_cast(a.ValGrerj as numeric) as valor_grerj,
  safe_cast(a.valJurosPri as numeric) as valor_juros_principal,
  safe_cast(a.valJurosHon as numeric) as valor_juros_honorarios,
  safe_cast(a.descObservacao as string) as observacoes,
  safe_cast(a.dtEmissao as date) as data_emissao,
  safe_cast(a.dtPagamento as date) as data_pagamento,
  safe_cast(a.valPrincipalPago as numeric) as valor_principal_pago,
  safe_cast(a.valJurosPago as numeric) as valor_juros_pago,
  safe_cast(a.ValHonorariosPag as numeric) as valor_honorarios_pago,
  safe_cast(a.ValGrerjPag as numeric) as valor_grerj_pago,
  safe_cast(a.valJurosPagoHon as numeric) as valor_juros_honorarios_pago,
  safe_cast(a.anoIPCAe as int64) as ano_ipcae,
  a._airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'CotaPagamento') }} a
left join {{ source('brutos_divida_ativa_staging', 'Cotas_Associadas') }} b 
  on b.numGuiaPagamentoAssoc = a.numGuiaPagamento and b.numCotaPagamentoAssoc = a.numCotaPagamento