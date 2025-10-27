{{
    config(
        alias='certidao_divida_ativa' 
    )
}}

with 

devedores_cda as 
(
  select a.id_certidao_divida_ativa, 
    count(b.id_pessoa) as quantidade_devedores_vinculados,
    array_agg(
      struct(
        a.id_pessoa, 
        b.cpf_cnpj, 
        b.tipo_pessoa, 
        b.descricao_tipo_pessoa, 
        b.nome
      ) order by b.tipo_pessoa, b.nome
    ) as devedores_vinculados_cda
  from {{ ref('raw_divida_ativa_devedor_cda') }} a
  inner join {{ ref('raw_divida_ativa_pessoa') }} b on b.id_pessoa = a.id_pessoa
  where a.codigo_tipo_vinculo = 'P'
  group by a.id_certidao_divida_ativa
),

imovel_associado as
(
  select safe_cast(a.inscricao_imobiliaria as int64) as inscricao_imobiliaria,
    struct(
      struct(
        a.codigo_logradouro,
        a.nome_logradouro,
        a.numero_porta,
        a.complemento_endereco,
        a.bairro,
        a.cep
      ) as endereco,
      struct(
        a.id_tipologia_imovel,
        a.nome_tipologia_imovel
      ) as tipologia_imovel,
      struct(
        a.id_utilizacao_imovel,
        a.nome_utilizacao_imovel
      ) as utilizacao_imovel
    ) as imovel_associado
  from {{ ref('raw_divida_ativa_imovel') }} a
),

guias_pagamento_x_cda as 
(
  select a.id_certidao_divida_ativa,
    count(b.id_guia_pagamento) as quantidade_guias_associadas, 
    array_agg(
      struct(
        c.id_guia_pagamento, 
        c.data_criacao_guia, 
        c.numero_processo_associado,
        struct(
          c.id_tipo_guia, 
          c.nome_tipo_guia
        ) as tipo_guia,
        struct(
          c.id_tipo_pagamento, 
          c.nome_tipo_pagamento
        ) as tipo_pagamento,
        struct(
          c.codigo_receita,
          c.nome_receita
        ) as tipo_receita,
        struct(
          c.id_situacao_guia_pagamento,
          c.nome_situacao_guia_pagamento
        ) as situacao,
        b.valor_cda_na_guia, 
        b.valor_desconto_cda
      ) order by c.data_criacao_guia
    ) as guias_pagamento_associadas
  from {{ ref('raw_divida_ativa_certidao_divida_ativa') }} a
  left join {{ ref('raw_divida_ativa_guia_pagamento_x_cda') }} b 
    on b.id_certidao_divida_ativa = a.id_certidao_divida_ativa
    and b.situacao_associacao = true -- apenas vínculo ativo entre guia de pagamento e CDA
  left join {{ ref('raw_divida_ativa_guia_pagamento') }} c 
    on c.id_guia_pagamento = b.id_guia_pagamento
    and c.id_situacao_guia_pagamento != 1 -- excluindo guias de pagamento canceladas
  group by a.id_certidao_divida_ativa
)

select a.id_certidao_divida_ativa,
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
  struct(
    a.codigo_receita_cda,
    a.nome_receita
  ) as tipo_receita,
  struct(
    a.id_entidade_credora,
    a.nome_entidade_credora
  ) as entidade_credora,
  struct(
    a.codigo_fase_cobranca,
    a.nome_fase_cobranca
  ) as fase_cobranca,
  struct(
    a.id_situacao_cda,
    a.descricao_situacao_cda
  ) as situacao_cda,
  struct(
    a.id_natureza_divida,
    a.nome_natureza_divida
  ) as natureza_divida_ativa,
  a.dados_complementares,
  ifnull(c.quantidade_devedores_vinculados, 0) as quantidade_devedores_cda,
  c.devedores_vinculados_cda,
  case 
    when d.inscricao_imobiliaria is null then false
    else true
  end as possui_imovel_associado,
  d.imovel_associado,
  b.quantidade_guias_associadas,
  b.guias_pagamento_associadas
from {{ ref('raw_divida_ativa_certidao_divida_ativa') }} a
left join guias_pagamento_x_cda b on b.id_certidao_divida_ativa = a.id_certidao_divida_ativa
left join devedores_cda c on c.id_certidao_divida_ativa = a.id_certidao_divida_ativa
left join imovel_associado d on d.inscricao_imobiliaria = a.inscricao_imobiliaria_imovel_associado_iptu
where a.id_situacao_cda not in (2, 98) -- Excluindo CDA's canceladas
