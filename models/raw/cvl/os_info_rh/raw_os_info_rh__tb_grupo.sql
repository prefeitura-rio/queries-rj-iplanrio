{{
    config(
      alias="tb_grupo",
      description="Grupos de usuários ou funcionários definidos para fins de controle de acesso ou alocação."
    )
}}

select
    safe_cast(`RHGR_TP_TIPO` as string) as grupo_tipo,
    safe_cast(`RHGR_CD_CODIGO` as integer) as grupo_codigo,
    safe_cast(`RHGR_NM_NOME` as string) as grupo_nome,
    safe_cast(`RHGR_IN_TOTALIZAR` as string) as grupo_totalizar,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'tb_grupo') }}
