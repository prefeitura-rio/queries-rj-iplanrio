{{
    config(
        alias='vinculo',
        materialized="table",
        tags=["raw", "ergon", "vinculo"],
        description="Tabela que contém os registros dos vínculos funcionais da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    a.id_funcionario,
    a.id_vinculo,
    a.id_matricula_vinculo,
    a.tipo_vinculo,
    a.data_nomeacao,
    a.data_posse,
    a.data_exercicio,
    a.regime_juridico,
    a.categoria,
    a.desconta_ir,
    a.descricao_provimento,
    a.classificacao_concurso,
    a.data_inicio_cessao,
    a.data_fim_cessao,
    a.data_concurso,
    a.data_fgts,
    a.data_inicio_contrato,
    a.data_fim_contrato,
    a.data_prorrogacao_contrato,
    a.data_aposentadoria,
    a.tipo_aposentadoria,
    a.data_vacancia,
    a.id_forma_vacancia,
    a.motivo_vacancia,
    a.id_vinculo_anterior,
    a.id_vinculo_posterior,
    a.tipo_orgao_origem,
    a.funcao_origem_requisicao,
    a.telefone_requisicao,
    a.orgao_origem_requisicao,
    a.tipo_onus_requisicao,
    a.tipo_ressarcimento_requisicao,
    a.tipo_requisicao,
    a.data_pagamento_requisicao,
    a.origem_servidor_cedido,
    a.id_processo_origem,
    a.contrato_suspenso,
    a.cota,
    a.id_empresa,
    a.data_homologacao_aposentadoria,
    a.updated_at as data_atualizacao
FROM {{ ref('raw_recursos_humanos_ergon__vinculo')}} a -- pegando como origem o dbt original da SMA que faz o tratamento dos vínculos 
inner join (
    select distinct id_empresa from {{ ref('mart_recursos_humanos_ergon_pgm__setor')}} 
    ) b on b.id_empresa = a.id_empresa -- e combinando apenas com os empresas dos setores da PGM que foram tratados neste outro dbt, para garantir que traga apenas os vínculos relacionados à PGM
inner join (
    select distinct id_funcionario, id_vinculo from {{ ref('mart_recursos_humanos_ergon_pgm__provimento')}} 
    ) c on c.id_funcionario = a.id_funcionario and c.id_vinculo = a.id_vinculo -- e combinando apenas com os funcionários que possuem provimentos relacionados à PGM, para garantir que traga apenas os vínculos relacionados à PGM
