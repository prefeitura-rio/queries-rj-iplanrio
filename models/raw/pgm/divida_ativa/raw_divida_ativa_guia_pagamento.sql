{{
    config(
        schema="brutos_divida_ativa",
        alias="guia_pagamento",
        materialized="table",
        tags=["raw", "divida_ativa", "guia_pagamento", "guia", "pagamento"],
        description="Tabela que contém os registros das guias de pagammentos geradas para parcelamento de dívidas. Uma guia de pagamento pode relacionar mais de uma CDA."
    )
}}

select safe_cast(a.numGuiaPagamento as int64) as id_guia_pagamento,
  safe_cast(a.numSeqGuia as int64) as sequencial_guia,
  safe_cast(a.datCriacao as datetime) as data_criacao_guia,
  safe_cast(a.codTipoGuia as int64) as id_tipo_guia,
  case safe_cast(a.codTipoGuia as int64)
    when 1 then 'Guia Principal'
    when 3 then 'Guia de Honorários'
    else 'Não classificada'
  end as nome_tipo_guia,
  safe_cast(a.codReceita as string) as codigo_receita,
  e.nome_receita,
  safe_cast(a.numProcessoAdministrativo as string) as numero_processo_associado,
  safe_cast(a.qtdParcelasPagas as int64) as quantidade_parcelas_pagas,
  safe_cast(a.datUltPagamento as date) as data_ultimo_pagamento,
  safe_cast(a.ValTotalDivida as numeric) as valor_total_divida,
  safe_cast(a.ValTotalPago as numeric) as valor_total_pago,
  safe_cast(a.qtdCDAGuia as int64) as quantidade_cdas_associadas,
  safe_cast(a.NomRequerente as string) as nome_requerente,
  safe_cast(a.NumIdentRequerente as string) as numero_documento,
  safe_cast(a.numTipoDoc as int64) as tipo_documento_requerente,
  case safe_cast(a.numTipoDoc as int64)
    when 0 then 'Identidade'
    when 1 then 'CPF/CNPJ'
    else 'Não identificado'
  end as nome_tipo_documento_requerente,
  safe_cast(a.NumTelRequerente as string) as telefone_requerente,
  safe_cast(a.DescEmailRequerente as string) as email_requetente,
  safe_cast(a.DescEmailEnvio as string) as email_envio_correspondencia,
  safe_cast(a.DescEnderecoEnvio as string) as endereco_envio_correspondencia,
  safe_cast(a.numEnderecoEnvio as string) as numero_porta_envio_correspondencia,
  safe_cast(a.descComplementoEnvio as string) as complemento_endereco_envio_correspondencia,
  safe_cast(a.descBairro as string) as bairro_envio_correspondencia,
  safe_cast(a.numCEPEnvio as string) as cep_envio_correspondencia,
  safe_cast(a.descCidade as string) as municipio_envio_correspondencia,
  safe_cast(a.SigUFEnvio as string) as uf_envio_correspondencia,
  safe_cast(a.idSituacaoGuia as int64) as id_situacao_guia_pagamento,
  ifnull(b.nome_situacao_guia_pagamento, 'Não identificado') as nome_situacao_guia_pagamento,
  safe_cast(a.codTipoPagamento as int64) as id_tipo_pagamento_guia,
  c.texto_amigavel,
  c.texto_judicial,
  c.texto_amigavel_todas_parcelas,
  c.texto_judicial_todas_parcelas,
  c.texto_amigavel_primeiras_guias,
  c.texto_judicial_primeiras_guias,
  safe_cast(a.idpessoa as int64) as id_pessoa,
  d.cpf_cnpj,
  d.nome,
  d.tipo_pessoa,
  d.descricao_tipo_pessoa,
  safe_cast(a.ValPrincipal as numeric) as valor_principal,
  safe_cast(a.ValHonorarios as numeric) as valor_honorarios,
  safe_cast(ValGrerj as numeric) as valor_grerj,
  safe_cast(a.ValPrincipalPag as numeric) as valor_principal_pago,
  safe_cast(a.ValHonorariosPag as numeric) as valor_honorarios_pago,
  safe_cast(a.ValGrerjPag as numeric) as valor_grerj_pago,
  safe_cast(a.qtdParcelasHon as int64) as quantidade_parcelas_honorarios,
  safe_cast(a.qtdParcelasHonPag as int64) as quantidade_parcelas_honorarios_pagas,
  safe_cast(a.qtdParcelasGrerj as int64) as quantidade_parcelas_grerj,
  safe_cast(a.qtdParcelasGrerjPag as int64) as quantidade_parcelas_grerj_pagas,
  a._airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at  
from {{ source('brutos_divida_ativa_staging', 'GuiaPagamento') }} a
left join {{ ref('raw_divida_ativa_situacao_guia_pagamento') }} b on b.id_situacao_guia_pagamento = a.idSituacaoGuia
left join {{ ref('raw_divida_ativa_tipo_pagamento_guia') }} c on c.id_tipo_pagamento_guia = a.codTipoPagamento
left join {{ ref('raw_divida_ativa_pessoa') }} d on d.id_pessoa = a.idPessoa
left join {{ ref('raw_divida_ativa_tipo_receita') }} e on e.codigo_receita = a.codReceita
