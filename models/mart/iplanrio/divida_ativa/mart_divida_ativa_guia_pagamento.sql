{{
    config(
        alias='guia_pagamento',
        materialized="table" 
    )
}}

with 
cotas_substitutas_pagas as
(
  select *
  from {{ ref('raw_divida_ativa_cota_pagamento') }} a --rj-iplanrio.brutos_divida_ativa.cota_pagamento
  where a.substituta_de_outra and a.cota_paga
),

cotas_substitutas_a_vencer as
(
  select *
  from {{ ref('raw_divida_ativa_cota_pagamento') }} a --rj-iplanrio.brutos_divida_ativa.cota_pagamento
  where a.substituta_de_outra and not a.cota_paga 
  and a.data_vencimento >= current_date("America/Sao_Paulo")
), 

cotas_jamais_substituidas as 
(
  select *
  from {{ ref('raw_divida_ativa_cota_pagamento') }} a --rj-iplanrio.brutos_divida_ativa.cota_pagamento
  where a.cota_substituida = false
),

cotas_substitutas_validas as 
(
  select * 
  from cotas_substitutas_pagas
  union distinct
  select *
  from cotas_substitutas_a_vencer
),

cotas_originais_substituidas_nao_pagas as
(
  select distinct a.*
  from {{ ref('raw_divida_ativa_cota_pagamento') }} a --rj-iplanrio.brutos_divida_ativa.cota_pagamento
  inner join {{ ref('raw_divida_ativa_cota_pagamento_associado') }} b --rj-iplanrio.brutos_divida_ativa.cota_pagamento_associada
    on b.id_guia_pagamento_associada = a.id_guia_pagamento and b.id_cota_guia_pagamento_associada = a.id_cota_guia_pagamento
  left join cotas_substitutas_validas c
    on c.id_guia_pagamento = b.id_guia_pagamento and c.id_cota_guia_pagamento = b.id_cota_guia_pagamento
  where c.id_guia_pagamento is null
  and not a.cota_paga
),

cotas_validas as 
(
  select * from cotas_jamais_substituidas
  union distinct 
  select * from cotas_substitutas_validas
  union distinct
  select * from cotas_originais_substituidas_nao_pagas
),

cotas_por_guia as 
(
  select a.id_guia_pagamento, 
    count(a.id_cota_guia_pagamento) as quantidade_cotas,
    array_agg(
      struct(
        a.id_cota_guia_pagamento,
        a.cota_paga,
        a.cota_substituida,
        a.substituta_de_outra,
        a.valor_cota_guia_pagamento,
        a.data_emissao,
        a.data_vencimento,
        a.data_pagamento,
        a.valor_principal,
        a.valor_honorarios,
        a.valor_juros,
        a.valor_grerj,
        a.valor_juros_principal,
        a.valor_juros_honorarios,
        a.observacoes,
        a.valor_principal_pago,
        a.valor_juros_pago,
        a.valor_honorarios_pago,
        a.valor_grerj_pago,
        a.valor_juros_honorarios_pago,
        a.ano_ipcae
      ) order by a.data_vencimento
    ) as cotas
  from cotas_validas a --rj-iplanrio.brutos_divida_ativa.guia_pagamento
  group by a.id_guia_pagamento
),

cdas_por_guia as 
(
  select a.id_guia_pagamento, 
    count(c.id_certidao_divida_ativa) as quantidade_cdas,
    array_agg(
      struct(
        c.id_certidao_divida_ativa,
        c.ano_de_inscricao_na_divida, 
        c.data_geracao_cda, 
        c.data_ultima_alteracao_situacao, 
        c.numero_processo_associado,
        c.valor_original_divida_ativa, 
        c.valor_saldo_devido, 
        c.valor_multa_moratoria, 
        c.valor_juros_moratorios, 
        c.valor_mora, 
        c.valor_juros_mora, 
        c.valor_pago_principal, 
        c.valor_honorarios, 
        c.valor_mora_smf_iptu,
        c.tipo_receita,
        c.entidade_credora,
        c.fase_cobranca,
        c.situacao_cda,
        c.natureza_divida_ativa,
        c.dados_complementares,
        c.quantidade_devedores_cda,
        c.devedores_vinculados_cda,
        c.possui_imovel_associado,
        c.imovel_associado
      ) order by c.id_certidao_divida_ativa
    ) as cdas_associadas
  from {{ ref('raw_divida_ativa_guia_pagamento') }} a --rj-iplanrio.brutos_divida_ativa.guia_pagamento
  inner join {{ ref('raw_divida_ativa_guia_pagamento_x_cda') }} b --rj-iplanrio.brutos_divida_ativa.guia_pagamento_x_cda
    on b.id_guia_pagamento = a.id_guia_pagamento
   and b.situacao_associacao = true -- apenas vínculo ativo entre guia de pagamento e CDA
  inner join {{ ref('mart_divida_ativa_certidao_divida_ativa') }} c --rj-iplanrio.divida_ativa.certidao_divida_ativa
    on c.id_certidao_divida_ativa = b.id_certidao_divida_ativa
   and c.situacao_cda.id_situacao_cda not in (2, 98)
  where a.id_situacao_guia_pagamento <> 1
  group by a.id_guia_pagamento
),

guias as (
  select a.id_guia_pagamento, 
    a.data_criacao_guia, 
    a.numero_processo_associado,
    struct(
      a.id_tipo_guia, 
      a.nome_tipo_guia
    ) as tipo_guia,
    struct(
      a.id_tipo_pagamento, 
      a.nome_tipo_pagamento
    ) as tipo_pagamento,
    struct(
      a.codigo_receita,
      a.nome_receita
    ) as tipo_receita,
    struct(
      a.id_situacao_guia_pagamento,
      a.nome_situacao_guia_pagamento
    ) as situacao,
    b.quantidade_cotas,
    b.cotas,
    c.quantidade_cdas,
    c.cdas_associadas
  from {{ ref('raw_divida_ativa_guia_pagamento') }} a --rj-iplanrio.brutos_divida_ativa.guia_pagamento
  left join cotas_por_guia b
    on b.id_guia_pagamento = a.id_guia_pagamento
  left join cdas_por_guia c
    on c.id_guia_pagamento = a.id_guia_pagamento
  where a.id_situacao_guia_pagamento <> 1
)

select * from guias
