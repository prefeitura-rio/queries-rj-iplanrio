{{
    config(
        alias="descontos",
        description="Tabela de Descontos",
    )
}}

select
    safe_cast(id as string) as id_desconto_associado,
    safe_cast(description as string) as descricao,
    safe_cast(value as float64) as desconto,
    safe_cast(createdat as date) as data_criacao,
    _airbyte_extracted_at as datalake_loaded_at,    
    current_timestamp() as datalake_transformed_at   

from {{ source("brutos_taxirio_staging", "discounts") }}
