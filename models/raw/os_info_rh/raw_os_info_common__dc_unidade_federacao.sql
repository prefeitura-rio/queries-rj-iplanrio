{{
    config(
      alias="dc_unidade_federacao",
      description="Tabela que armazena as possíveis unidades federativas no preenchimento de um endereço. DML executada direto no banco de dados."
    )
}}

select
    safe_cast(`UF_CD_IBGE` as string) as uf_ibge_codigo,
    safe_cast(`UF_SG_SIGLA` as string) as uf_sigla,
    safe_cast(`UF_DS_DESCRICAO` as string) as uf_descricao,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_common', 'dc_unidade_federacao') }}
