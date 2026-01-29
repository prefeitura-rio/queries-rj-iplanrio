{{
    config(
      alias="categoria",
      description="Tabela que armazena os possíveis categorias.",
      materialized='table'
    )
}}

select
    safe_cast(`IDCATEGORIA` as int64) as id_categoria,
    safe_cast(`COD_CATEGORIA` as string) as codigo_categoria,
    safe_cast(`DS_CATEGORIA` as string) as descricao_categoria,
    safe_cast(`DT_CADASTRO` as datetime) as data_cadastro,
    safe_cast(`DT_ULTIMA_ATUALIZ` as datetime) as data_ultima_atualizacao,
    safe_cast(`FLG_ATIVO` as char(1)) as flag_ativo,
    safe_cast(`CNES_OBRIGATORIO` as char(1)) as flag_cnes_obrigatorio,
    DATETIME(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    DATETIME(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'categoria') }}