{{
    config(
      alias="valores_provisionamento",
      description="Valores detalhados de verbas provisionadas mensalmente para cada funcionário.",
      materialized='table'
    )
}}

select
    safe_cast(`PROV_CD_PROVISIONAMENTO` as integer) as provisionamento_codigo,
    safe_cast(`RHCO_COD_COLUNA` as string) as coluna_codigo,
    safe_cast(`VLPR_VL_VALOR` as float64) as valor,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_valores_provisionamento') }}
