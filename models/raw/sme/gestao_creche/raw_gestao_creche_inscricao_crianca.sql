{{
    config(
        --schema="brutos_gestao_creche",
        alias="inscricao_crianca",
        materialized="table",
        tags=["raw", "gestao_creche", "inscricao_crianca", "ICH_InscricaoCrianca"],
        description="Relaciona as informações de uma inscrição em creche à criança solicitante."
    )
}}

SELECT safe_cast(prm_id as int64) as identificador_processo_matricula,
    safe_cast(plm_id as int64) as identificador_polo_matricula,
    safe_cast(ipl_id as int64) as identificador_inscricao_polo,    
    safe_cast(icr_nome as string) as nome,
    safe_cast(icr_DNV as string) as dnv,
    safe_cast(icr_dataNascimento as date) as data_nascimento,
    safe_cast(icr_sexo as string) as sexo,
    safe_cast(icr_situacao as int64) as situacao,
    safe_cast(icr_dataCriacao as datetime) as data_criacao,
    safe_cast(icr_dataAlteracao as datetime) as data_alteracao,
    safe_cast(icr_gemeoID as int64) as gemeo_id,
    safe_cast(icr_NIS as string) as nis,
    safe_cast(icr_CPF as string) as cpf,    
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_creche_staging', 'ICH_InscricaoCrianca') }}