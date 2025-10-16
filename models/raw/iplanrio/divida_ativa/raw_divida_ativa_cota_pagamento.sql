{{
    config(
        schema="brutos_divida_ativa",
        alias="cota_pagamento",
        materialized="table",
        tags=["raw", "divida_ativa", "cota_pagamento", "cota", "pagamento"],
        description="Tabela que contém os registros das cotas referentes às parcelas das guias de pagammentos geradas para parcelamento de dívidas. Uma guia de pagamento pode relacionar mais de uma CDA."
    )
}}

with 
  cotas_substituidas as (
    select 
      id_guia_pagamento_associada,
      id_cota_guia_pagamento_associada,
      count(1) as vezes_substituida
    from {{ ref('raw_divida_ativa_cota_pagamento_associado') }}
    group by id_guia_pagamento_associada, id_cota_guia_pagamento_associada
    having count(1) > 0
    ),

  cotas_substitutas as (
    select 
      id_guia_pagamento,
      id_cota_guia_pagamento,
      count(1) as vezes_que_substitui_outra
    from {{ ref('raw_divida_ativa_cota_pagamento_associado') }}
    group by id_guia_pagamento, id_cota_guia_pagamento
    having count(1) > 0
    )

select safe_cast(a.numGuiaPagamento as int64) as id_guia_pagamento,
  safe_cast(a.numCotaPagamento as int64) as id_cota_guia_pagamento,
  case safe_cast(a.dtPagamento as date)
    when null then false
    else true
  end as cota_paga,
  case safe_cast(b.vezes_substituida as int64)
    when null then false
    else true
  end as cota_substituida,
  case safe_cast(c.vezes_que_substitui_outra as int64)
    when null then false
    else true
  end as substituta_de_outra,
  safe_cast(a.valCotaPagamento as numeric) as valor_cota_guia_pagamento,
  safe_cast(a.dtVencimento as date) as data_vencimento,
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
  a._prefect_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging_prefect', 'CotaPagamento') }} a
left join cotas_substituidas b on CAST(b.id_guia_pagamento_associada as string) = a.numGuiaPagamento and CAST(b.id_cota_guia_pagamento_associada as string) = a.numCotaPagamento
left join cotas_substitutas c on CAST(c.id_guia_pagamento as string) = a.numGuiaPagamento and CAST(c.id_cota_guia_pagamento as string) = CAST(a.numCotaPagamento as string)