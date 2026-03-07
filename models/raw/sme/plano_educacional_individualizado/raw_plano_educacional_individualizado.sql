{{
    config(        
        alias="plano_educacional_individualizado",
        materialized="table",
        tags=["raw", "plano_educacional_individualizado", "PEI"],
        description="Contém os detalhes do Plano Educacional Individualizado"
    )
}}

SELECT safe_cast(pei_id as int64) as id_plano_educacional_individualizado,
    safe_cast(alu_id as int64) as id_aluno,
    safe_cast(dis_idSga as int64) as id_sigla_disciplina,
    safe_cast(pei_coc as int64) as conselho_classe_plano_educacional_individualizado,
    safe_cast(pei_ano as int64) as ano_plano_educacional_individualizado,
    safe_cast(pei_descEstrategia as string) as descricao_estrategia_plano_educacional_individualizado,
    safe_cast(pei_descRecursosEspecificos as string) as descricao_recursos_especifico_plano_educacional_individualizado,
    safe_cast(pei_descResultadoEsperado as string) as descricao_resultado_especifico_plano_educacional_individualizado,
    safe_cast(tur_idAtual as int64) as id_atual_turma,
    safe_cast(pei_objetivo as string) as objetivo_plano_educacional_individualizado,
    safe_cast(dt_inclusao_registro as datetime) as data_inclusao_registro,
    safe_cast(dis_idPEI as int64) as disciplina_plano_educacional_individualizado,
    safe_cast(cur_id as int64) as id_curriculo,
    safe_cast(crr_id as int64) as id_crr,
    safe_cast(crp_id as int64) as id_crp,
    safe_cast(pei_relDeAvaliacao as string) as relatorio_avaliacao_plano_educacional_individualizado,
    safe_cast(usu_LoginAvaliador as string) as login_usuario_avaliador,
    safe_cast(pei_DataAvaliacao as datetime) as data_avaliacao_plano_educacional_individualizado,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_plano_educacional_individualizado_staging', 'PEI') }}
