-- CONSISTÊNCIA DE APONTAMENTOS 

-- 1. Incoerência de Grave: condição grave ativa, mas resultado não é 'Grave'
SELECT
  id_documento,
  extracao_resultado,
  apontamento_resultado,
  'Erro: Condição Grave ativa com apontamento_resultado diferente de Grave' AS motivo_falha
FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
WHERE (
    apontamento_duplicidade_indicador IS TRUE
    OR apontamento_cancelamento_indicador IS TRUE
    OR apontamento_cnpj_inativo_indicador IS TRUE
    OR apontamento_valor_pago_excedente_indicador IS TRUE
    OR apontamento_emissao_anterior_abertura_indicador IS TRUE
    OR extracao_resultado = 'NF Não Encontrada'
  )
  AND apontamento_resultado <> 'Grave'

UNION ALL

-- 2. Falso Grave: rotulado como 'Grave' sem nenhuma condição grave ativa
SELECT
  id_documento,
  extracao_resultado,
  apontamento_resultado,
  'Erro: Resultado Grave sem indicador grave ativo e sem ser NF Nao Encontrada' AS motivo_falha
FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
WHERE apontamento_resultado = 'Grave'
  AND extracao_resultado <> 'NF Não Encontrada'
  AND NOT (
    apontamento_duplicidade_indicador IS TRUE
    OR apontamento_cancelamento_indicador IS TRUE
    OR apontamento_cnpj_inativo_indicador IS TRUE
    OR apontamento_valor_pago_excedente_indicador IS TRUE
    OR apontamento_emissao_anterior_abertura_indicador IS TRUE
  )

UNION ALL

-- 3. Incoerência de Leve: rotulado como 'Leve', mas possui flag grave ou nenhuma flag leve
SELECT
  id_documento,
  extracao_resultado,
  apontamento_resultado,
  'Erro: Resultado Leve com indicador grave ativo ou sem indicador leve' AS motivo_falha
FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
WHERE apontamento_resultado = 'Leve'
  AND (
    -- Não pode ter flag grave
    apontamento_duplicidade_indicador IS TRUE
    OR apontamento_cancelamento_indicador IS TRUE
    OR apontamento_cnpj_inativo_indicador IS TRUE
    OR apontamento_valor_pago_excedente_indicador IS TRUE
    OR apontamento_emissao_anterior_abertura_indicador IS TRUE
    -- Deve ter ao menos uma leve
    OR NOT (
      apontamento_emissao_divergente_indicador IS TRUE
      OR apontamento_valor_divergente_indicador IS TRUE
    )
  )

UNION ALL

-- 4. Incoerência de Descartado: rotulado como 'Descartado', mas tem indicador ativo
SELECT
  id_documento,
  extracao_resultado,
  apontamento_resultado,
  'Erro: Resultado Descartado com indicador leve ou grave ativo' AS motivo_falha
FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
WHERE apontamento_resultado = 'Descartado'
  AND (
    apontamento_duplicidade_indicador IS TRUE
    OR apontamento_cancelamento_indicador IS TRUE
    OR apontamento_cnpj_inativo_indicador IS TRUE
    OR apontamento_valor_pago_excedente_indicador IS TRUE
    OR apontamento_emissao_anterior_abertura_indicador IS TRUE
    OR apontamento_emissao_divergente_indicador IS TRUE
    OR apontamento_valor_divergente_indicador IS TRUE
  )

UNION ALL

-- 5. Incoerência de 'Não avaliado': descasamento bidirecional com 'NF Sem Match'
SELECT
  id_documento,
  extracao_resultado,
  apontamento_resultado,
  'Erro: Descasamento entre NF Sem Match e resultado Nao avaliado' AS motivo_falha
FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
WHERE (
    extracao_resultado = 'NF Sem Match' 
    AND apontamento_resultado <> 'Não avaliado'
  )
  OR (
    apontamento_resultado = 'Não avaliado' 
    AND extracao_resultado <> 'NF Sem Match'
  )