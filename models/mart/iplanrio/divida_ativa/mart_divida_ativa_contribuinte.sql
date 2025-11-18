{{
    config(
        alias='contribuinte',
        materialized="table" 
    )
}}

with

contribuintes as 
(
  select distinct b.id_pessoa, 
    b.cpf_cnpj,
    struct( 
        b.tipo_pessoa, 
        b.descricao_tipo_pessoa
    ) as tipo_pessoa,
    b.nome
  from {{ ref('mart_divida_ativa_certidao_divida_ativa') }} a, --`rj-iplanrio.divida_ativa.certidao_divida_ativa`
  unnest(devedores_vinculados_cda) b
),

cdas_contribuinte as 
(
  select b.id_pessoa,
    count(a.id_certidao_divida_ativa) as quantidade_cdas,
    sum(a.valor_saldo_devido) as saldo_devido_cdas,
    array_agg(
      struct(
        a.id_certidao_divida_ativa,
        a.ano_de_inscricao_na_divida, 
        a.data_geracao_cda, 
        a.data_ultima_alteracao_situacao, 
        a.numero_processo_associado,
        a.valor_original_divida_ativa, 
        a.valor_saldo_devido, 
        a.valor_multa_moratoria, 
        a.valor_juros_moratorios, 
        a.valor_mora, 
        a.valor_juros_mora, 
        a.valor_pago_principal, 
        a.valor_honorarios, 
        a.valor_mora_smf_iptu,
        a.tipo_receita,
        a.entidade_credora,
        a.fase_cobranca,
        a.situacao_cda,
        a.natureza_divida_ativa,
        a.dados_complementares,
        a.quantidade_devedores_cda,
        a.devedores_vinculados_cda,
        a.possui_imovel_associado,
        a.imovel_associado
      ) order by a.id_certidao_divida_ativa
    ) as cdas_associadas,
  from {{ ref('mart_divida_ativa_certidao_divida_ativa') }} a, -- `rj-iplanrio.divida_ativa.certidao_divida_ativa`
  unnest(devedores_vinculados_cda) b
  group by b.id_pessoa
),

contribuintes_vs_guias as
(
  select distinct c.id_pessoa, b.id_guia_pagamento
  from {{ ref('mart_divida_ativa_certidao_divida_ativa') }} a, -- `rj-iplanrio.divida_ativa.certidao_divida_ativa`
  unnest(a.guias_pagamento_associadas) b,
  unnest(a.devedores_vinculados_cda) c
),

guias_contribuinte as
 (
  select a.id_pessoa, 
    count(b.id_guia_pagamento) as quantidade_guias,
    array_agg(
      struct(
        b.id_guia_pagamento, 
        b.data_criacao_guia, 
        b.numero_processo_associado,
        b.tipo_guia,
        b.tipo_pagamento,
        b.tipo_receita,
        b.situacao,
        b.quantidade_cotas,
        b.cotas
      ) order by data_criacao_guia
    ) as guias_pagamento
  from contribuintes_vs_guias a
  inner join {{ ref('mart_divida_ativa_guia_pagamento') }} b --`rj-iplanrio.divida_ativa.guia_pagamento`
    on b.id_guia_pagamento = a.id_guia_pagamento
  group by a.id_pessoa
 )

select a.id_pessoa, 
    a.cpf_cnpj, 
    a.tipo_pessoa, 
    a.nome,
    b.quantidade_cdas,
    b.saldo_devido_cdas,
    b.cdas_associadas,
    c.quantidade_guias,
    c.guias_pagamento
from contribuintes a
left join cdas_contribuinte b 
  on b.id_pessoa = a.id_pessoa
left join guias_contribuinte c 
  on c.id_pessoa = a.id_pessoa

