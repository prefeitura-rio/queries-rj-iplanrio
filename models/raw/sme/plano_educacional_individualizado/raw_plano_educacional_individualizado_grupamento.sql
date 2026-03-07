{{
    config(        
        alias="grupamento",
        materialized="table",
        tags=["raw", "plano_educacional_individualizado", "grupamento", "PEI_Grupamento"],
        description="Tabela de grupamentos do PEI"
    )
}}

SELECT safe_cast(gru_id as int64) as id_grupamento,
    safe_cast(gru_numero as int64) as numero_grupamento,
    safe_cast(gru_descricao as string) as descricao_grupamento,
    safe_cast(gru_idadeIdealInicio as int64) as idade_Ideal_Inicio_grupamento,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_plano_educacional_individualizado_staging', 'PEI_Grupamento') }}
