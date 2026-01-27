{{
    config(
      alias="tb_valores_provisionamento",
      description="Valores detalhados de verbas provisionadas mensalmente para cada funcionário."
    )
}}

select
    safe_cast(`PROV_CD_PROVISIONAMENTO` as integer) as provisionamento_codigo,
    safe_cast(`RHCO_COD_COLUNA` as integer) as coluna_codigo,
    safe_cast(`VLPR_VL_VALOR` as float64) as valor,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_valores_provisionamento') }}
