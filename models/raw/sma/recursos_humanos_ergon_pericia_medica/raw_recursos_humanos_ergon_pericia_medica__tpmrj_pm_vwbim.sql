{{
    config(
        alias='tpmrj_pm_vwbim',
        schema='brutos_ergon_staging'
    )
}}

SELECT  rowid_reg,
 id_bim, id_solic_bim, numfunc, numvinc, matric, nome, tipovinc, cargo, desc_cargo, setor, nomesetor, readaptado, tipo_pessoa, 
    numdep, dt_solicitacao, requerimento, eh_reassuncao, email, telefone, nome_dependente, motivo_inspecao, situacao, situacao_desc, faltando_servico,
    faltando_servico_desde, situacao_funcional_regular, horario_trabalho_ini, horario_trabalho_fim, observacao_rh, motivo_rejeicao, artigo, 
    dias_dif_agendamento 
FROM {{ source('recursos_humanos_ergon_pericia_medica_staging', 'tpmrj_pm_vwbim ') }}

