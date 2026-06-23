{{
    config(
        schema="dashboard_taxirio",
        alias="corridas",
        materialized="table",
        tags=["mart", "taxirio", "corridas", "dashboard"],
        description="Tabela que contém uma abstração das corridas registradas no sistema de taxirio, com tratamento de dados para análise."
    )
}}

with 

corridas AS (
  SELECT
  id_corrida,
  id_municipio,
  id_motorista,
  nota_avaliacao as avaliacao,
  CASE id_pagamento_associado
    WHEN '5ef0b65815f5d3cae84f9862' THEN 'Mercado Pago'
    WHEN '5a26edc4b3e31e6203d0ba97' THEN 'Cartão de débito'
    WHEN '5a26ecd5b3e31e6203d0ac11' THEN 'Dinheiro'
    WHEN '617ab4e7a89e0d3f6649f925' THEN 'PIX'
    WHEN '5a26ed84b3e31e6203d0b6d9' THEN 'Cartão de Crédito Taxi'
    WHEN '62bf49abb7fa45275fc8bd86' THEN 'Arariboia'
    WHEN '5aeb5abb019fbeb1a71d1cc1' THEN 'Corporativo Prefeitura RJ'
    WHEN '5aeb5abb019fbeb1a71d1cc0' THEN 'Cartão de Crédito App'
    WHEN '5a99619d331ab758dc58e444' THEN 'Corporativo'
    ELSE 'Indefinido'
  END AS metodo_pagamento,
  CASE id_desconto_associado
    WHEN '59c424f09c4b6aa933bbed94' THEN '10%'
    WHEN '59c424f09c4b6aa933bbed95' THEN '20%'
    WHEN '59c424f09c4b6aa933bbed96' THEN '30%'
    WHEN '59c424f09c4b6aa933bbed97' THEN '40%'
    WHEN '59c424f09c4b6aa933bbed93' THEN 'Tarifa Normal'
    ELSE 'Indefinido'
  END AS desconto_associado,
  CASE 
    WHEN id_pagamento_associado = '5ef0b65815f5d3cae84f9862' THEN 'QR CODE'
    WHEN id_pagamento_associado in ('5a26edc4b3e31e6203d0ba97', 
                                    '5a26ecd5b3e31e6203d0ac11', 
                                    '617ab4e7a89e0d3f6649f925', 
                                    '5a26ed84b3e31e6203d0b6d9', 
                                    '62bf49abb7fa45275fc8bd86') THEN 'Pagamento no táxi'
    WHEN id_pagamento_associado in ('5aeb5abb019fbeb1a71d1cc1', 
                                    '5aeb5abb019fbeb1a71d1cc0', 
                                    '5a99619d331ab758dc58e444') THEN 'Pagamento no aplicativo'
    ELSE 'Indefinido'
  END AS tipo_pagamento,
  CASE 
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b31','5b2b9080b5595a3a2fbd8b3c') THEN 'Finalizada'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b48',
                    '630cd45decfde68291af4115',
                    '630cfb48ecfde682919990d9',
                    '5b50e7365fef9b0ac0a1bd79',
                    '630cfb33ecfde68291990f07',
                    '5b2b9080b5595a3a2fbd8b47',
                    '5b2b9080b5595a3a2fbd8b46',
                    '5b2b9080b5595a3a2fbd8b50',
                    '630cd44becfde68291aec097') THEN 'Retificadora'      
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b45',
                    '5b2b9080b5595a3a2fbd8b4f',
                    '5b2b9080b5595a3a2fbd8b38',
                    '5b2b9080b5595a3a2fbd8b4e') THEN 'Estornada'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b32',
                    '630cd433ecfde68291ae20a3', 
                    '630cfb23ecfde6829198a9f0') THEN 'Cancelada pelo Taxista'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b33', '5b2b9080b5595a3a2fbd8b4b') THEN 'Passageiro Cancela Antes começar'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b34',
                    '630cd41decfde68291ad916a', 
                    '630cfb13ecfde682919848d6') THEN 'Passageiro Cancela Depois começar'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b44',
                    '5b2b9080b5595a3a2fbd8b4c', 
                    '5b2b9080b5595a3a2fbd8b4d', 
                    '5b2b9080b5595a3a2fbd8b43', 
                    '5b2b9080b5595a3a2fbd8b42') THEN 'Faturada'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b3d', 
                    '5b2b9080b5595a3a2fbd8b3f', 
                    '5b2b9080b5595a3a2fbd8b40', 
                    '5b2b9080b5595a3a2fbd8b41', 
                    '5c533bfa8ab8ac610018a360', 
                    '5b2b9080b5595a3a2fbd8b3e', 
                    '5c533be78ab8ac610018a321', 
                    '5c533bf18ab8ac610018a336' ) THEN 'Inválida'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b3a', '64f0dcddf0e55d564b3a19cd') THEN 'Não atendida'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b3b') THEN 'Aguardando Taxista'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b35') THEN 'Em Andamento'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b39') THEN 'Em Aberto'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b37', '5b2b9080b5595a3a2fbd8b36') THEN 'Em análise'
    WHEN id_evento IN ('5b4d4061add99882d9042c1a') THEN 'Erro pagamento'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b49') THEN 'Pagamento não autorizado'
    WHEN id_evento IN ('5b2b9080b5595a3a2fbd8b4a') THEN 'Pré autorização sem retorno'
    ELSE 'Indefinido'
  END AS status_corrida_para_dashboard,
  status as status_corrida,
  cast(timestamp_sub(data_criacao, INTERVAL 3 HOUR) as date) AS data_corrida,
  cast(timestamp_sub(data_criacao, INTERVAL 3 HOUR) as time) AS hora_corrida,
  ifnull(valor_total_a_pagar, 0) as preco_corrida_com_pedagio,
  ifnull(valor_total_pedagio, 0) as valor_pedagio,
  CASE
    WHEN ifnull(valor_total_com_desconto, 0) > 6.0 THEN valor_total_com_desconto
    ELSE 6
  END AS preco_com_desconto_calculado,
  ifnull(valor_total_com_desconto, 0) AS preco_com_desconto_bruto,
  ifnull(valor_total_sem_desconto, 0) AS preco_sem_desconto
   FROM {{ ref('raw_taxirio_races') }}
),

cidades AS (
  SELECT
  id_municipio,
  nome_municipio
  FROM {{ ref('raw_taxirio_cities') }}
)

SELECT 
  a.id_corrida,
  a.id_motorista,
  a.metodo_pagamento,
  a.desconto_associado,
  a.tipo_pagamento,
  a.status_corrida_para_dashboard,
  a.status_corrida,
  a.data_corrida,
  a.hora_corrida,
  a.avaliacao,
  a.preco_corrida_com_pedagio,
  a.valor_pedagio,
  a.preco_com_desconto_bruto,
  a.preco_com_desconto_calculado,
  a.preco_sem_desconto,
  ifnull(1 - SAFE_DIVIDE(ifnull(a.preco_corrida_com_pedagio, 0) - ifnull(a.valor_pedagio, 0), ifnull(a.preco_sem_desconto, 0)), 0) AS desconto_efetivo,
  case when b.nome_municipio is null then 'NÃO IDENTIFICADO' ELSE b.nome_municipio end as nome_municipio
FROM corridas a
left JOIN cidades b ON a.id_municipio = b.id_municipio