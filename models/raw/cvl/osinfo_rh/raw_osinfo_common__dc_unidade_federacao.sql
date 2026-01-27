{{
    config(
      alias="dc_unidade_federacao",
      description="Tabela que armazena as possíveis unidades federativas no preenchimento de um endereço. DML executada direto no banco de dados."
    )
}}

select
    safe_cast(`UF_CD_IBGE` as decimal) as uf_ibge_codigo,
    safe_cast(`UF_SG_SIGLA` as string) as uf_sigla,
    safe_cast(`UF_DS_DESCRICAO` as string) as uf_descricao,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_unidade_federacao') }}
