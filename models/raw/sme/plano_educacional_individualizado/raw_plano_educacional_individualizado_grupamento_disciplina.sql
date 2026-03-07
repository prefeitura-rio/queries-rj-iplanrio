{{
    config(        
        alias="grupamento_disciplina",
        materialized="table",
        tags=["raw", "plano_educacional_individualizado", "grupamento_disciplina", "GrupamentoDisciplina"],
        description="Grupamentos da Disciplina"
    )
}}

SELECT safe_cast(gru_id as int64) as id_grupamento,
    safe_cast(dis_id as int64) as id_disciplina,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_plano_educacional_individualizado_staging', 'PEI_GrupamentoDisciplina') }}
