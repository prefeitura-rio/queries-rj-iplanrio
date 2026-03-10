{{
    config(        
        alias="habilidade",
        materialized="view",
        tags=["raw", "plano_educacional_individualizado", "habilidade", "PEI_Habilidade"],
        description="Habilidades Avaliadas no PEI"
    )
}}

SELECT safe_cast(hab_id as int64) as id_habilidade,
    safe_cast(gru_id as int64) as id_grupamento,
    safe_cast(dis_id as int64) as id_disciplina,
    safe_cast(eix_id as int64) as id_eixo,
    safe_cast(hab_descricao as string) as descricao_habilidade,
    safe_cast(hab_nrBimestre as int64) as nr_bimestre_habilidade,
    safe_cast(hab_codigo as string) as codigo_habilidade,
    safe_cast(hab_situacao as int64) as situacao_habilidade,
    safe_cast(hab_DataCriacao as datetime) as data_criacao_habilidade,
    safe_cast(hab_DataAlteracao as datetime) as data_alteracao_habilidade,
    safe_cast(hab_ehPEI as boolean) as eh_pei,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_plano_educacional_individualizado_staging', 'PEI_Habilidade') }}
