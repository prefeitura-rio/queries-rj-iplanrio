{{
    config(
        alias='empresas',
        materialized="table",
        tags=["raw", "ergon", "empresas"],
        description="Tabela que contém os registros das empresas da administração direta ou indireta da prefeitura do Rio de Janeiro."
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
    updated_at as data_atualizacao
FROM {{ ref('raw_recursos_humanos_ergon__empresas') }} -- pegando como origem o dbt original de tratamento das empresas da SMA apenas com setores que mencionam a PGM, para garantir que traga apenas os registros relacionados à PGM em sua sigla.
where CAST(id_empresa as string) in ('1', '2', '12') -- apenas empresas ligadas a setors que apresentam a palavra PGM em sua sigla.