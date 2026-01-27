{{
    config(
      alias="tb_cbo",
      description="Ocupações da Classificação Brasileira de Ocupações (CBO) com seus códigos e descrições oficiais."
    )
}}

select
    safe_cast(`CBO_CD_CBO` as string) as cbo_codigo,
    safe_cast(`CBO_DS_CBO` as string) as cbo_descricao,
    safe_cast(`CBO_IN_ATIVIDADE_FIM` as integer) as cbo_atividade_fim,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_cbo') }}
