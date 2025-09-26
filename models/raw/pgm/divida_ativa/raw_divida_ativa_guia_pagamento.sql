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
  struct(
    safe_cast(a.codTipoGuia as int64) as id_tipo_guia,
    case safe_cast(a.codTipoGuia as int64)
      when 1 then 'Guia Principal'
      when 3 then 'Guia de Honorários'
      else 'Não classificada'
    end as nome_tipo_guia
  ) as tipo_guia,
  struct(
    safe_cast(a.codReceita as string) as codigo_receita,
    e.nome_receita
  ) as receita,
  safe_cast(a.numProcessoAdministrativo as string) as numero_processo_associado,
  safe_cast(a.qtdParcelasPagas as int64) as quantidade_parcelas_pagas,
  safe_cast(a.datUltPagamento as date) as data_ultimo_pagamento,
  safe_cast(a.ValTotalDivida as numeric) as valor_total_divida,
  safe_cast(a.ValTotalPago as numeric) as valor_total_pago,
  safe_cast(a.qtdCDAGuia as int64) as quantidade_cdas_associadas,
  struct(
    safe_cast(a.NomRequerente as string) as nome_requerente,
    struct(
      safe_cast(a.NumIdentRequerente as string) as numero_documento,
      safe_cast(a.numTipoDoc as int64) as tipo_documento_requerente,
      case safe_cast(a.numTipoDoc as int64)
        when 0 then 'Identidade'
        when 1 then 'CPF/CNPJ'
        else 'Não identificado'
      end as nome_tipo_documento_requerente
    ) as documento_requerente,
    safe_cast(a.NumTelRequerente as string) as telefone_requerente,
    safe_cast(a.DescEmailRequerente as string) as email_requetente
  ) as requerente,
  struct(
    safe_cast(a.DescEmailEnvio as string) as email_envio,
    safe_cast(a.DescEnderecoEnvio as string) as endereco_envio,
    safe_cast(a.numEnderecoEnvio as string) as numero_porta_envio,
    safe_cast(a.descComplementoEnvio as string) as complemento_endereco_envio,
    safe_cast(a.descBairro as string) as bairro_envio,
    safe_cast(a.numCEPEnvio as string) as cep_envio,
    safe_cast(a.descCidade as string) as municipio_envio,
    safe_cast(a.SigUFEnvio as string) as uf_envio
  ) as envio_correspondencia,
  struct(
    safe_cast(a.idSituacaoGuia as int64) as id_situacao_guia_pagamento,
    ifnull(b.nome_situacao_guia_pagamento, 'Não identificado') as nome_situacao_guia_pagamento
  ) as situacao_guia,
  struct(
    safe_cast(a.codTipoPagamento as int64) as id_tipo_pagamento_guia,
    c.texto_amigavel,
    c.texto_judicial,
    c.texto_amigavel_todas_parcelas,
    c.texto_judicial_todas_parcelas,
    c.texto_amigavel_primeiras_guias,
    c.texto_judicial_primeiras_guias    
  ) as tipo_pagamento,
  struct(
    safe_cast(a.idpessoa as int64) as id_pessoa,
    d.cpf_cnpj,
    d.nome,
    d.tipo_pessoa,
    d.descricao_tipo_pessoa
  ) as devedor_indicado,
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
  array_agg(
    struct(
      f.id_certidao_divida_ativa, 
      f.situacao_associacao,
      f.descricao_situacao_associacao,
      f.data_retirada_associacao,
      f.valor_desconto_cda,
      f.ano_referencia_ipcae,
      f.valor_cda_na_guia,
      f.percentual_desconto,
      f.valor_desconto_sobre_principal
    )
  ) as cdas_associadas,
/*  array_agg(
    struct(
      g.id_certidao_divida_ativa, 
      g.id_guia_pagamento,
      g.situacao_associacao,
      g.descricao_situacao_associacao,
      g.data_retirada_associacao,
      g.valor_desconto_honorario,
      g.ano_referencia_ipcae,
      g.valor_honorario_na_guia,
      g.percentual_desconto
    )
  ) as honorarios_associados,*/
  a._airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at  
from {{ source('brutos_divida_ativa_staging', 'GuiaPagamento') }} a
left join {{ ref('raw_divida_ativa_situacao_guia_pagamento') }} b on b.id_situacao_guia_pagamento = a.idSituacaoGuia
left join {{ ref('raw_divida_ativa_tipo_pagamento_guia') }} c on c.id_tipo_pagamento_guia = a.codTipoPagamento
left join {{ ref('raw_divida_ativa_pessoa') }} d on d.id_pessoa = a.idPessoa
left join {{ ref('raw_divida_ativa_tipo_receita') }} e on e.codigo_receita = a.codReceita
left join {{ ref('raw_divida_ativa_guia_pagamento_x_cda') }} f on f.id_guia_pagamento = a.numGuiaPagamento
--left join {{ ref('raw_divida_ativa_guia_pagamento_x_honorario') }} g on g.id_guia_pagamento = a.numGuiaPagamento
group by a.numGuiaPagamento, 
a.numSeqGuia,
a.datCriacao,
a.codTipoGuia,
a.codReceita,
e.nome_receita,
a.numProcessoAdministrativo,
a.qtdParcelasPagas,
a.datUltPagamento,
a.ValTotalDivida,
a.ValTotalPago,
a.qtdCDAGuia,
a.NomRequerente,
a.NumIdentRequerente,
a.numTipoDoc,
a.NumTelRequerente,
a.DescEmailRequerente,
a.DescEmailEnvio,
a.DescEnderecoEnvio,
a.numEnderecoEnvio,
a.descComplementoEnvio,
a.descBairro,
a.numCEPEnvio,
a.descCidade,
a.SigUFEnvio,
a.idSituacaoGuia,
b.nome_situacao_guia_pagamento,
a.codTipoPagamento,
c.texto_amigavel,
c.texto_judicial,
c.texto_amigavel_todas_parcelas,
c.texto_judicial_todas_parcelas,
c.texto_amigavel_primeiras_guias,
c.texto_judicial_primeiras_guias,
a.idpessoa,
d.cpf_cnpj,
d.nome,
d.tipo_pessoa,
d.descricao_tipo_pessoa,
a.ValPrincipal,
a.ValHonorarios,
a.ValGrerj,
a.ValPrincipalPag,
a.ValHonorariosPag,
a.ValGrerjPag,
a.qtdParcelasHon,
a.qtdParcelasHonPag,
a.qtdParcelasGrerj,
a.qtdParcelasGrerjPag,
a._airbyte_extracted_at
