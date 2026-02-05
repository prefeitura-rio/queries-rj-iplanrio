{{
    config(
      alias="categoria",
      description="Tabela que armazena os possíveis categorias.",
      materialized="view"
          )
}}

select
    safe_cast(`IDCATEGORIA` as int64) as id_categoria,
    safe_cast(`COD_CATEGORIA` as string) as codigo_categoria,
    safe_cast(`DS_CATEGORIA` as string) as descricao_categoria,
    safe_cast(`DT_CADASTRO` as datetime) as data_cadastro,
    safe_cast(`DT_ULTIMA_ATUALIZ` as datetime) as data_ultima_atualizacao,
    safe_cast(`FLG_ATIVO` as string) as flag_ativo,
    safe_cast(`CNES_OBRIGATORIO` as string) as flag_cnes_obrigatorio,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'categoria') }}