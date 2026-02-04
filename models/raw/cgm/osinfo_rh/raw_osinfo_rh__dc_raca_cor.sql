{{
    config(
      alias="raca_cor",
      description="Classificações de raça ou cor utilizadas para fins de censo e registros governamentais.",
    )
}}

select
    safe_cast(`RACO_CD_RACA_COR` as int64) as raca_cor_codigo,
    safe_cast(`RACO_DS_DESCRICAO` as string) as raca_cor_descricao,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_raca_cor') }}
