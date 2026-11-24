-- CONSISTÊNCIA DE APONTAMENTOS (MART CGM)
-- 1. Incoerência de Grave: condição grave ativa, mas resultado não é 'Grave'
select
    id_documento_osinfo,
    extracao_resultado_agente_ia,
    apontamento_resultado,
    'Erro: Condição Grave ativa com apontamento_resultado diferente de Grave'
    as motivo_falha
from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
where
    (
        apontamento_duplicidade_indicador is true
        or apontamento_cancelamento_indicador is true
        or apontamento_cnpj_inativo_indicador is true
        or extracao_resultado_agente_ia = 'NF Não Encontrada'
    )
    and apontamento_resultado <> 'Grave'

union all

-- 2. Falso Grave: rotulado como 'Grave' sem nenhuma condição grave ativa
select
    id_documento_osinfo,
    extracao_resultado_agente_ia,
    apontamento_resultado,
    'Erro: Resultado Grave sem indicador grave ativo e sem ser NF Não Encontrada'
    as motivo_falha
from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
where
    apontamento_resultado = 'Grave'
    and extracao_resultado_agente_ia <> 'NF Não Encontrada'
    and not (
        apontamento_duplicidade_indicador is true
        or apontamento_cancelamento_indicador is true
        or apontamento_cnpj_inativo_indicador is true
    )

union all

-- 3. Incoerência de Leve: rotulado como 'Leve', mas possui flag grave ou nenhuma flag
-- leve
select
    id_documento_osinfo,
    extracao_resultado_agente_ia,
    apontamento_resultado,
    'Erro: Resultado Leve com indicador grave ativo ou sem indicador leve'
    as motivo_falha
from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
where
    apontamento_resultado = 'Leve'
    and (
        -- Não pode ter flag grave
        apontamento_duplicidade_indicador is true
        or apontamento_cancelamento_indicador is true
        or apontamento_cnpj_inativo_indicador is true
        -- Deve ter ao menos uma leve
        or not (
            apontamento_emissao_divergente_indicador is true
            or apontamento_valor_divergente_indicador is true
        )
    )

union all

-- 4. Incoerência de Descartado: rotulado como 'Descartado', mas tem indicador ativo
select
    id_documento_osinfo,
    extracao_resultado_agente_ia,
    apontamento_resultado,
    'Erro: Resultado Descartado com indicador leve ou grave ativo' as motivo_falha
from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
where
    apontamento_resultado = 'Descartado'
    and (
        apontamento_duplicidade_indicador is true
        or apontamento_cancelamento_indicador is true
        or apontamento_cnpj_inativo_indicador is true
        or apontamento_emissao_divergente_indicador is true
        or apontamento_valor_divergente_indicador is true
    )

union all

-- 5. Incoerência de 'Não avaliado': descasamento bidirecional com 'NF Sem Match'
select
    id_documento_osinfo,
    extracao_resultado_agente_ia,
    apontamento_resultado,
    'Erro: Descasamento entre NF Sem Match e resultado Não avaliado' as motivo_falha
from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
where
    (
        extracao_resultado_agente_ia = 'NF Sem Match'
        and apontamento_resultado <> 'Não avaliado'
    )
    or (
        apontamento_resultado = 'Não avaliado'
        and extracao_resultado_agente_ia <> 'NF Sem Match'
    )
