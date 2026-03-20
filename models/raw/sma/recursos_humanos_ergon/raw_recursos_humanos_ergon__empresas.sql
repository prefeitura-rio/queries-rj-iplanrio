{{
    config(
        alias='empresas',
        materialized="table",
        tags=["raw", "ergon", "empresas"],
        description="Tabela que contém os registros das empresas da administração direta ou indireta da prefeitura do Rio de Janeiros."
    )
}}

SELECT
    {{ clean_and_cast('EMPRESA', 'int64') }} AS id_empresa,
    SAFE_CAST(FLEX_CAMPO_07 AS STRING) AS tipo_empresa,
    SAFE_CAST(NOME AS STRING) AS nome_empresa,
    SAFE_CAST(FANTASIA AS STRING) AS sigla,
    SAFE_CAST(RAZAO AS STRING) AS razao_social,
    {{ clean_and_cast('CGC', 'string') }} AS cnpj,
    {{ clean_and_cast('ATIV_ECON', 'string') }} AS atividade_economica,
    SAFE_CAST(CEP AS STRING) AS cep,
    {{ clean_and_cast('CNAE', 'string') }} AS cnae,
    {{ clean_and_cast('FONE', 'string') }} AS telefone,
    SAFE_CAST(EMAIL AS STRING) AS email,
    safe_cast(web as string) as website,
    {{ clean_and_cast('NAT_JUR', 'string') }} AS natureza_juridica,
    SAFE_CAST(CPF_RESP AS STRING) AS cpf_resp,
    SAFE_CAST(RESPONSAVEL AS STRING) AS responsavel,
    {{ clean_and_cast('LOG_CODIGO', 'string') }} AS codigo_logradouro,
    SAFE_CAST(NOMELOGRADOURO AS STRING) AS nome_logradouro,
    SAFE_CAST(TIPOLOGRADOURO AS STRING) AS tipo_logradouro,
    {{ clean_and_cast('NUMENDER', 'string') }} AS numero_endereco,
    SAFE_CAST(COMPLEMENTO AS STRING) AS complemento,
    SAFE_CAST(MBAIRRO_CODIGO AS STRING) AS bairro,
    SAFE_CAST(MUNICIPIO_CODIGO AS STRING) AS municipio_codigo,
    SAFE_CAST(MUNICIPIO AS STRING) AS municipio_sede,
    SAFE_CAST(UF_SIGLA AS STRING) AS uf_sigla,
    SAFE_CAST(_airbyte_extracted_at AS timestamp) AS updated_at
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_EMPRESAS')}}