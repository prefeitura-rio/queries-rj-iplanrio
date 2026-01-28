{{
    config(
      alias="grupo",
      description="Grupos de usuários ou funcionários definidos para fins de controle de acesso ou alocação.",  
    )
}}

select
    safe_cast(`RHGR_TP_TIPO` as string) as grupo_tipo,
    safe_cast(`RHGR_CD_CODIGO` as string) as grupo_codigo,
    safe_cast(`RHGR_NM_NOME` as string) as grupo_nome,
    safe_cast(`RHGR_IN_TOTALIZAR` as int) as grupo_totalizar,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_grupo') }}
