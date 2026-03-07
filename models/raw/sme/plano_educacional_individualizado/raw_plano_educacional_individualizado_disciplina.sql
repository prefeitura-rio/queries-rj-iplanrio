{{
    config(        
        alias="disciplina",
        materialized="table",
        tags=["raw", "plano_educacional_individualizado", "disciplina", "PEIDisciplina"],
        description="Disciplinas a serem usadas no PEI"
    )
}}

SELECT safe_cast(dis_id as int64) as id_disciplina,
    safe_cast(dis_nome as string) as nome_disciplina,
    safe_cast(tme_id as int64) as id_nivel,
    safe_cast(tne_id as int64) as id_modalidade,
    safe_cast(dis_ehPEI as boolean) as flag_discilina_plano_educacional,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_plano_educacional_individualizado_staging', 'PEI_Disciplina') }}
