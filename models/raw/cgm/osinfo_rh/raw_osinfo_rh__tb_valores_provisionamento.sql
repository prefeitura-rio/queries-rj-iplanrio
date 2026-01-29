{{
    config(
      alias="valores_provisionamento",
      description="Valores detalhados de verbas provisionadas mensalmente para cada funcionário.",
      materialized='table'
    )
}}

select
    safe_cast(`PROV_CD_PROVISIONAMENTO` as int64) as provisionamento_codigo,
    safe_cast(`RHCO_COD_COLUNA` as string) as coluna_codigo,
    safe_cast(`VLPR_VL_VALOR` as numeric) as valor,
    DATETIME(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    DATETIME(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_valores_provisionamento') }}
