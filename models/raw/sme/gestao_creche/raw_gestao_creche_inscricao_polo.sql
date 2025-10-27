{{
    config(        
        alias="inscricao_polo",
        materialized="table",
        tags=["raw", "gestao_creche", "polo", "ICH_InscricaoPolo"],
        description="Apresenta as informações relativas às inscrições realizadas em pólos disponiblizados."
    )
}}

SELECT safe_cast(ipl_id as int64) as identificador_inscricao_polo,
    safe_cast(prm_id as int64) as identificador_processo_matricula,
    safe_cast(plm_id as int64) as identificador_polo_matricula,    
    safe_cast(cur_id as int64) as identificador_curriculo,
    safe_cast(crr_id as int64) as identificador_requisito_curriculo,
    safe_cast(crp_id as int64) as identificador_curriculo_periodo,
    safe_cast(ipl_codigoFicha as string) as codigo_ficha,
    safe_cast(ipl_grauVulnerabilidade as int64) as grau_vulnerabilidade,
    safe_cast(ipl_numeroSorteio as string) as numero_sorteio,
    safe_cast(ipl_classificacaoPolo as int64) as classificacao_polo,
    safe_cast(ipl_esc_id as int64) as esc_id,
    safe_cast(ipl_uni_id as int64) as uni_id,
    safe_cast(ipl_situacao as int64) as situacao,    
    safe_cast(ipl_dataCriacao as datetime) as data_criacao,
    safe_cast(ipl_dataAlteracao as datetime) as data_alteracao,
    safe_cast(ipl_participarPIC as boolean) as participar_pic,
    safe_cast(ipl_grup_Saida_Parceira as string) as grupo_saida_parceira,  
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_creche_staging', 'ICH_InscricaoPolo') }}