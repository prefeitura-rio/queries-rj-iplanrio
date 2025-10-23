{{
    config(
        --schema="brutos_gestao_creche",
        alias="inscricao_creche",
        materialized="table",
        tags=["raw", "gestao_creche", "inscricao_creche", "ICH_InscricaoCreche"],
        description="Apresenta informações sobre as inscrições realizadas para uma creche."
    )
}}

SELECT safe_cast(ich_id as int64) as identificador_inscricao_creche,
    safe_cast(prm_id as int64) as identificador_processo_matricula,
    safe_cast(plm_id as int64) as identificador_polo_matricula,
    safe_cast(ipl_id as int64) as identificador_inscricao_polo,
    safe_cast(esc_id as int64) as identificador_escola,
    safe_cast(uni_id as int64) as identificador_unidade_escolar,
    safe_cast(ich_preferenciaCreche as int64) as preferencia_creche,
    safe_cast(ich_picAnoAnterior as int64) as pic_ano_anterior,
    safe_cast(ich_listaAnoAnterior as int64) as lista_ano_anterior,
    safe_cast(ich_dataConvocacao as date) as data_convocacao,
    safe_cast(ich_observacao as string) as observacao,
    safe_cast(ich_situacao as int64) as situacao,
    safe_cast(ich_dataCriacao as datetime) as data_criacao,
    safe_cast(ich_dataAlteracao as datetime) as data_alteracao,
    safe_cast(ich_justificativaConvocacaoExcepcional as string) as justificativa_convocacao_excepcional,
    safe_cast(ich_statusConvocacaoExcepcional as int64) as status_convocacao_excepcional,
    safe_cast(ich_dataConvocacaoExcepcional as datetime) as data_convocacao_excepcional,
    safe_cast(ich_dataCancelamentoConvocacaoExcepcional as datetime) as data_cancelamento_convocacao_excepcional,
    safe_cast(ich_horarioIntegral as bool) as horario_integral,
    safe_cast(ich_horarioParcial as bool) as horario_parcial,
    safe_cast(ich_situacaoIntegral as int64) as situacao_integral,
    safe_cast(ich_situacaoParcial as int64) as situacao_parcial,
    safe_cast(ich_dataConvocacao2 as datetime) as data_convocacao2,
    safe_cast(ich_dataLimiteConvocacao as datetime) as data_limite_convocacao,
    safe_cast(ich_comprovanteConvocacaoArq as bytes) as comprovante_convocacao_arq,
    safe_cast(ich_comprovanteConvocacaoNome as string) as comprovante_convocacao_nome,
    safe_cast(ich_EscolaAnteriorId as int64) as escola_anterior_id,
    safe_cast(ich_TipoJustificativaAlocacaoOutraListaDeEspera as int64) as tipo_justificativa_alocacao_outra_lista_espera,
    safe_cast(ich_JustificativaAlocacaoOutraListaDeEspera as string) as justificativa_alocacao_outra_lista_espera,
    safe_cast(ich_dataSaida as date) as data_saida,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_creche_staging', 'ICH_InscricaoCreche') }}