{{
    config(
      alias="categoria",
      description="Tabela que armazena os possíveis categorias."
    )
}}

select
    safe_cast(`IDCATEGORIA` as decimal) as IDENTIFICADOR_CATEGORIA,
    safe_cast(`COD_CATEGORIA` as string) as CODIGO_CATEGORIA,
    safe_cast(`DS_CATEGORIA` as string) as DESCRICAO_CATEGORIA,
    safe_cast(`DT_CADASTRO` as datetime) as DATA_CADASTRO,
    safe_cast(`DT_ULTIMA_ATUALIZ` as datetime) as DATA_ULTIMA_ATUALIZACAO,
    safe_cast(`FLG_ATIVO` as boolean) as FLAG_ATIVO,
    safe_cast(`CNES_OBRIGATORIO` as boolean) as FLAG_CNES_OBRIGATORIO
FROM {{ source('brutos_osinfo_rh_staging', 'categoria') }}