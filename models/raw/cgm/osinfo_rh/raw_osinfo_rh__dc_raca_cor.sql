{{
    config(
      alias="raca_cor",
      description="Classificações de raça ou cor utilizadas para fins de censo e registros governamentais.",
      materialized='table'
    )
}}

select
    safe_cast(`RACO_CD_RACA_COR` as int64) as raca_cor_codigo,
    safe_cast(`RACO_DS_DESCRICAO` as string) as raca_cor_descricao,
    DATETIME(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    DATETIME(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_raca_cor') }}
