{{
    config(
        --schema="brutos_gestao_creche",
        alias="responsavel_endereco",
        materialized="table",
        tags=["raw", "gestao_creche", "responsavel_endereco", "ICH_ResponsavelEndereco"],
        description="Relaciona os responsáveis de uma criança e suas informações de endereo e contato"
    )
}}

SELECT safe_cast(prm_id as int64) as identificador_processo_matricula,
    safe_cast(plm_id as int64) as identificador_polo_matricula,
    safe_cast(ipl_id as int64) as identificador_inscricao_polo,
    safe_cast(ire_id as int64) as identificador_responsavel,
    safe_cast(end_id as int64) as identificador_endereco,
    safe_cast(ren_numero as string) as numero,
    safe_cast(ren_complemento as string) as complemento,
    safe_cast(ren_telefoneDomiciliar as string) as telefone_domiciliar,
    safe_cast(ren_telefoneTrabalho as string) as telefone_trabalho,
    safe_cast(ren_telefoneCelular as string) as telefone_celular,
    safe_cast(ren_telefoneRecado as string) as telefone_recado,
    safe_cast(ren_situacao as int64) as situacao,
    safe_cast(ren_dataCriacao as datetime) as data_criacao,
    safe_cast(ren_dataAlteracao as datetime) as data_alteracao,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_creche_staging', 'ICH_ResponsavelEndereco') }}