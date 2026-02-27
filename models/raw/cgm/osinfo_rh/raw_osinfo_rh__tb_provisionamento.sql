{{
    config(
      alias="provisionamento",
      description="Provisionamentos de verbas trabalhistas de funcionários para um período, garantindo a reserva contábil.",
      materialized='table'  
    )
}}

select
    safe_cast(`PROV_CD_PROVISIONAMENTO` as int64) as provisionamento_codigo,
    safe_cast(`VINC_CD_VINCULO` as int64) as vinculo_codigo,
    safe_cast(`PROV_NR_MES_REFERENCIA` as int64) as mes_referencia,
    safe_cast(`PROV_NR_ANO_REFERENCIA` as int64) as ano_referencia,
    safe_cast(`PROV_NR_MES_COMPETENCIA` as int64) as mes_competencia,
    safe_cast(`PROV_NR_ANO_COMPETENCIA` as int64) as ano_competencia,
    safe_cast(`ID_CONTRATO` as string) as contrato_id,
    safe_cast(`COD_UNIDADE` as string) as unidade_codigo,
    safe_cast(`PROV_OBSERVACAO` as string) as observacao,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_provisionamento') }}
