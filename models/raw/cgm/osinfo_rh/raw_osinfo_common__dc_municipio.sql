{{
    config(
      alias="municipio",
      description="Tabela que armazena os possíveis municípios no preenchimento de um endereço. DML executada direto no banco de dados.",
      materialized='view'
    )
}}

select
    safe_cast(`MUNI_CD_IBGE` as decimal) as municipio_ibge_codigo,
    safe_cast(`UF_CD_IBGE` as decimal) as uf_ibge_codigo,
    safe_cast(`MUNI_DS_NOME` as string) as municipio_nome,
    timestamp(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    timestamp(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_municipio') }}
