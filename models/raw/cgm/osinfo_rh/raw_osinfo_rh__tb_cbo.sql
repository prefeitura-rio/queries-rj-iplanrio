{{
    config(
      alias="cbo",
      description="Ocupações da Classificação Brasileira de Ocupações (CBO) com seus códigos e descrições oficiais.",
      materialized='table'
    )
}}

select
    safe_cast(`CBO_CD_CBO` as string) as cbo_codigo,
    safe_cast(`CBO_DS_CBO` as string) as cbo_descricao,
    safe_cast(`CBO_IN_ATIVIDADE_FIM` as int64) as cbo_atividade_fim,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
FROM {{ source('brutos_osinfo_rh_staging', 'tb_cbo') }}
