{{
    config(        
        alias="eixo",
        materialized="view",
        tags=["raw", "plano_educacional_individualizado", "eixo", "PEI_Eixo"],
        description="Eixos educacionais do PEI"
    )
}}

SELECT safe_cast(gru_id as int64) as id_grupamento,
    safe_cast(dis_id as int64) as id_disciplina,
    safe_cast(eix_id as int64) as id_eixo,
    safe_cast(eix_nome as string) as nome_eixo,
    safe_cast(eix_objetosDeConhecimento as string) as objetivo_conhecimento_eixo,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_plano_educacional_individualizado_staging', 'PEI_Eixo') }}
