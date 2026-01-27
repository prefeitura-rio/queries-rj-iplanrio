{{
    config(
      alias="municipio",
      description="Tabela que armazena os possíveis municípios no preenchimento de um endereço. DML executada direto no banco de dados."
    )
}}

select
    safe_cast(`MUNI_CD_IBGE` as decimal) as municipio_ibge_codigo,
    safe_cast(`UF_CD_IBGE` as string) as uf_ibge_codigo,
    safe_cast(`MUNI_DS_NOME` as string) as municipio_nome,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_municipio') }}
