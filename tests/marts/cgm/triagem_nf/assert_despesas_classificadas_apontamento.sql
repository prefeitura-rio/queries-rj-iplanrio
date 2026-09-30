-- Caso 1: Indicador Grave ativo com apontamento_resultado incorreto
select
    id_documento,
    extracao_resultado,
    apontamento_resultado,
    'Erro: Indicador Grave com resultado nao Grave' as motivo_falha
from {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
where (
    apontamento_duplicidade_indicador is true
    or apontamento_cancelamento_indicador is true
    or apontamento_valor_pago_excedente_indicador is true
    or apontamento_emissao_anterior_abertura_indicador is true
    or extracao_resultado = 'NF Não Encontrada'
)
and apontamento_resultado != 'Grave'

union all

-- Caso 2: NF Sem Match com apontamento_resultado diferente de 'Não avaliado'
select
    id_documento,
    extracao_resultado,
    apontamento_resultado,
    'Erro: NF Sem Match com resultado diferente de Nao avaliado' as motivo_falha
from {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
where extracao_resultado = 'NF Sem Match'
  and apontamento_resultado != 'Não avaliado'