{{
    config(
        schema="brutos_divida_ativa",
        alias="guia_pagamento_x_cda",
        materialized="table",
        tags=["raw", "divida_ativa", "guia_pagamento", "certidao_divida_ativa", "CDA"],
        description="Tabela que contém os registros das associações entre as CDA's e as guias de pagamento."
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
  safe_cast(a.ValDescontoCDA as numeric) as valor_desconto_cda,
  safe_cast(a.anoIPCAe as int64) as ano_referencia_ipcae,
  safe_cast(a.ValCDA as numeric) as valor_cda_na_guia,
  safe_cast(a.PercDesconto as numeric) as percentual_desconto,
  safe_cast(a.valDescontoSobrePrincipal as numeric) as valor_desconto_sobre_principal,
  a._airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'GuiaPagamento_CDA') }} a