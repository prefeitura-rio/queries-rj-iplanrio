{{
    config(
        schema="brutos_gestao_creche",
        alias="inscricao_responsavel",
        materialized="table",
        tags=["raw", "gestao_creche", "inscricao_responsavel", "ICH_InscricaoResponsável"],
        description="Relaciona as informações de uma inscrição em creche aos responsáveis pela criança."
    )
}}

SELECT safe_cast(ire_id as int64) as identificador_responsavel,
    safe_cast(prm_id as int64) as identificador_processo_matricula,
    safe_cast(plm_id as int64) as identificador_polo_matricula,
    safe_cast(ipl_id as int64) as identificador_inscricao_polo,
    safe_cast(ire_nome as string) as nome,
    safe_cast(ire_dataNascimento as date) as data_nascimento,
    safe_cast(ire_NIS as string) as nis,
    safe_cast(ire_tipo as int64) as tipo,
    safe_cast(ire_responsavel as boolean) as responsavel,
    safe_cast(ire_situacao as int64) as situacao,
    safe_cast(ire_dataCriacao as datetime) as data_criacao,
    safe_cast(ire_dataAlteracao as datetime) as data_alteracao,
    _prefect_extracted_at as loaded_at,
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_creche_staging', 'ICH_InscricaoResponsavel') }}