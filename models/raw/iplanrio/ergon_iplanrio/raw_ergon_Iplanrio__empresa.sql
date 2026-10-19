{{
    config(
        alias='empresa',
        materialized="table",
        tags=["raw", "ergon", "empresas"],
        description="Tabela que contém os registros das empresas do Ergon que fazem referência à IplanRio."
    )
}}

SELECT 
    id_empresa,
    tipo_empresa,
    nome_empresa,
    sigla,
    razao_social,
    cnpj,
    atividade_economica,
    cep,
    cnae,
    telefone,
    email,
    website,
    natureza_juridica,
    cpf_resp,
    responsavel,
    codigo_logradouro,
    nome_logradouro,
    tipo_logradouro,
    numero_endereco,
    complemento,
    bairro,
    municipio_codigo,
    municipio_sede,
    uf_sigla,
    updated_at
FROM {{ ref("raw_recursos_humanos_ergon__empresas") }}
where id_empresa = '18'