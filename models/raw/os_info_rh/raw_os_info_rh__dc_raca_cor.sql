{{
    config(
      alias="dc_raca_cor",
      description="Classificações de raça ou cor utilizadas para fins de censo e registros governamentais."
    )
}}

select
    safe_cast(`RACO_CD_RACA_COR` as integer) as raca_cor_codigo,
    safe_cast(`RACO_DS_DESCRICAO` as string) as raca_cor_descricao,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'dc_raca_cor') }}
