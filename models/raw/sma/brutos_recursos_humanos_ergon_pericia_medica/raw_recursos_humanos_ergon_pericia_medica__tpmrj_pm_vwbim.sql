{{
    config(
        alias='tpmrj_pm_vwbim',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "tpmrj_pm_vwbim"],
        description="Visão de boletins de inspeção médica."
    )
}}

SELECT
    SAFE_CAST(rowid_reg AS STRING) AS rowid_registro,
    SAFE_CAST(id_bim AS STRING) AS id_bim,
    SAFE_CAST(id_solic_bim AS STRING) AS id_solicitacao_bim,
    SAFE_CAST(numfunc AS STRING) AS id_funcionario,
    SAFE_CAST(numvinc AS STRING) AS id_vinculo,
    SAFE_CAST(emp_codigo AS STRING) AS id_empresa,
    SAFE_CAST(matric AS STRING) AS matricula,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(tipovinc AS STRING) AS tipo_vinculo,
    SAFE_CAST(cargo AS STRING) AS id_cargo,
    SAFE_CAST(desc_cargo AS STRING) AS nome_cargo,
    SAFE_CAST(setor AS STRING) AS setor,
    SAFE_CAST(nomesetor AS STRING) AS nome_setor,
    SAFE_CAST(readaptado AS STRING) AS readaptado,
    SAFE_CAST(tipo_pessoa AS STRING) AS tipo_pessoa,
    SAFE_CAST(numdep AS STRING) AS id_dependente,
    SAFE_CAST(dt_solicitacao AS DATE) AS data_solicitacao,
    SAFE_CAST(requerimento AS STRING) AS requerimento,
    SAFE_CAST(eh_reassuncao AS STRING) AS reassuncao,
    SAFE_CAST(email AS STRING) AS email,
    SAFE_CAST(telefone AS STRING) AS telefone,
    SAFE_CAST(nome_dependente AS STRING) AS nome_dependente,
    SAFE_CAST(motivo_inspecao AS STRING) AS motivo_inspecao,
    SAFE_CAST(situacao AS INT64) AS situacao,
    SAFE_CAST(situacao_desc AS STRING) AS descricao_situacao,
    SAFE_CAST(faltando_servico AS STRING) AS faltando_servico,
    SAFE_CAST(faltando_servico_desde AS DATE) AS faltando_servico_desde,
    SAFE_CAST(situacao_funcional_regular AS STRING) AS situacao_funcional_regular,
    SAFE_CAST(horario_trabalho_ini AS INT64) AS horario_trabalho_inicio,
    SAFE_CAST(horario_trabalho_fim AS INT64) AS horario_trabalho_fim,
    SAFE_CAST(observacao_rh AS STRING) AS observacao_rh,
    SAFE_CAST(motivo_rejeicao AS STRING) AS motivo_rejeicao,
    SAFE_CAST(artigo AS STRING) AS artigo,
    SAFE_CAST(dias_dif_agendamento AS STRING) AS dias_diferenca_agendamento,
    SAFE_CAST(doc_medico AS STRING) AS documento_medico,
    SAFE_CAST(dias_concedidos AS INT64) AS dias_concedidos,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'tpmrj_pm_vwbim') }} AS t
