{{
    config(
        alias='tpmrj_pm_vwbim'
    )
}}

SELECT  safe_cast(rowid_reg AS STRING) AS rowid_reg,
    safe_cast(id_bim AS STRING) AS id_bim,
    safe_cast(id_solic_bim AS STRING) AS id_solic_bim,
    safe_cast(numfunc AS STRING) AS numfunc,
    safe_cast(numvinc AS STRING) AS numvinc,
    safe_cast(matric AS STRING) AS matric,
    safe_cast(nome AS STRING) AS nome,
    safe_cast(tipovinc AS STRING) AS tipovinc,
    safe_cast(cargo AS STRING) AS cargo,
    safe_cast(desc_cargo AS STRING) AS desc_cargo,
    safe_cast(setor AS STRING) AS setor,
    safe_cast(nomesetor AS STRING) AS nomesetor,
    safe_cast(readaptado AS STRING) AS readaptado,
    safe_cast(tipo_pessoa AS STRING) AS tipo_pessoa, 
    safe_cast(numdep AS STRING) AS numdep, 
    safe_cast(dt_solicitacao AS STRING) AS dt_solicitacao,
    safe_cast(requerimento AS STRING) AS requerimento,
    safe_cast(eh_reassuncao AS STRING) AS eh_reassuncao,
    safe_cast(email AS STRING) AS email,
    safe_cast(telefone AS STRING) AS telefone,
    safe_cast(nome_dependente AS STRING) AS nome_dependente,
    safe_cast(motivo_inspecao AS STRING) AS motivo_inspecao,
    safe_cast(situacao AS STRING) AS situacao,
    safe_cast(situacao_desc AS STRING) AS situacao_desc,
    safe_cast(faltando_servico AS STRING) AS faltando_servico,
    safe_cast(faltando_servico_desde AS STRING) AS faltando_servico_desde,
    safe_cast(situacao_funcional_regular AS STRING) AS situacao_funcional_regular,
    safe_cast(horario_trabalho_ini AS STRING) AS horario_trabalho_ini,
    safe_cast(horario_trabalho_fim AS STRING) AS horario_trabalho_fim,
    safe_cast(observacao_rh AS STRING) AS observacao_rh,
    safe_cast(motivo_rejeicao AS STRING) AS motivo_rejeicao,
    safe_cast(artigo AS STRING) AS artigo,
    safe_cast(dias_dif_agendamento AS STRING) AS dias_dif_agendamento,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at  
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'TPMRJ_PM_VWBIM') }}

