{{
    config(
      alias="dc_municipio",
      description="Tabela que armazena os possíveis municípios no preenchimento de um endereço. DML executada direto no banco de dados."
    )
}}

select
    safe_cast(`MUNI_CD_IBGE` as string) as municipio_ibge_codigo,
    safe_cast(`UF_CD_IBGE` as string) as uf_ibge_codigo,
    safe_cast(`MUNI_DS_NOME` as string) as municipio_nome,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_common', 'dc_municipio') }}
