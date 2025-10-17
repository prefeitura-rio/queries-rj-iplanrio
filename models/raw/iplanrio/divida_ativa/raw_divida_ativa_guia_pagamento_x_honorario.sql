{{
    config(
        alias="guia_pagamento_x_honorario",
        materialized="table",
        tags=["raw", "divida_ativa", "guia_pagamento", "honorarios", "CDA"],
        description="Tabela que contém os registros das associações entre as honorários e as guias de pagamento."
    )
}}

select safe_cast(a.numCDA as int64) as id_certidao_divida_ativa,
  safe_cast(a.numGuiaPagamento as int64) as id_guia_pagamento,
  case safe_cast(a.FlgAtivo as string) 
    when 'S' then true
    else false
  end as situacao_associacao,
  case safe_cast(a.FlgAtivo as string)
    when 'S' then 'Associação ativa'
    else 'Associação Inativa'
  end as descricao_situacao_associacao,
  safe_cast(a.DatRetirada as date) as data_retirada_associacao,
  safe_cast(a.ValDesconto as numeric) as valor_desconto_honorario,
  safe_cast(a.anoIPCAe as int64) as ano_referencia_ipcae,
  safe_cast(a.ValHonorarios as numeric) as valor_honorario_na_guia,
  safe_cast(a.PercDesconto as numeric) as percentual_desconto,
  a._prefect_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging_prefect', 'GuiaPagamento_Honorarios') }} a