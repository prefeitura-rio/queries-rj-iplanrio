{{
    config(
        alias='cnpj' 
    )
}}

with cpf_table as (
   select cpf, nome from {{ ref("raw_bcadastro_cpf") }} as t
), 

socios_achatados as (
    SELECT 
        t.cnpj,
        STRUCT(socio.codigo_pais, socio.cpf_socio, pf.nome as nome_socio, socio.cnpj_socio, socio.cpf_representante_legal, socio.data_situacao_especial, socio.nome_socio_estrangeiro, socio.qualificacao_representante_legal, socio.qualificacao_socio, socio.tipo) AS socio_com_nome
    FROM {{ ref("raw_bcadastro_cnpj") }} as t, unnest(socios) as socio
    LEFT JOIN cpf_table AS pf 
        ON socio.cpf_socio = pf.cpf
), socios_enriquecidos as (
    SELECT 
        cnpj,
        ARRAY_AGG(socio_com_nome) AS socios
    FROM socios_achatados
    GROUP BY cnpj
)

select
    cnpj.cnpj,
    cnpj.razao_social,
    cnpj.nome_fantasia,
    cnpj.capital_social,
    cnpj.cnae_fiscal,
    cnpj.cnae_secundarias,
    cnpj.nire,
    struct(
        cnpj.natureza_juridica.id,
        cnpj.natureza_juridica.descricao
    ) as natureza_juridica,
    struct(
        cnpj.porte.id,
        cnpj.porte.descricao
    ) as porte,
    struct(
        cnpj.matriz_filial.id,
        cnpj.matriz_filial.descricao
    ) as matriz_filial,
    struct(
        cnpj.orgao_registro.id,
        cnpj.orgao_registro.descricao
    ) as orgao_registro,
    cnpj.inicio_atividade_data,
    struct(
        cnpj.situacao_cadastral.id,
        cnpj.situacao_cadastral.descricao,
        cnpj.situacao_cadastral.data,
        cnpj.situacao_cadastral.motivo_id,
        cnpj.situacao_cadastral.motivo_descricao
    ) as situacao_cadastral,
    struct(
        cnpj.situacao_especial.descricao,
        cnpj.situacao_especial.data
    ) as situacao_especial,
    struct(
        cnpj.ente_federativo.id,
        cnpj.ente_federativo.tipo
    ) as ente_federativo,
    struct(
        cnpj.contato.telefone as telefone,
        cnpj.contato.email
    ) as contato,
    struct(
        cnpj.endereco.cep,
        cnpj.endereco.id_pais,
        cnpj.endereco.uf,
        cnpj.endereco.id_municipio,
        cnpj.endereco.municipio_nome,
        cnpj.endereco.municipio_exterior_nome,
        cnpj.endereco.bairro,
        cnpj.endereco.tipo_logradouro,
        cnpj.endereco.logradouro,
        cnpj.endereco.numero,
        cnpj.endereco.complemento
    ) as endereco,
    struct(
        struct(
            cnpj.contador.pf.tipo_crc,
            cnpj.contador.pf.classificacao_crc,
            cnpj.contador.pf.sequencial_crc,
            cnpj.contador.pf.id
        ) as pf,
        struct(
            cnpj.contador.pj.tipo_crc,
            cnpj.contador.pj.classificacao_crc,
            cnpj.contador.pj.sequencial_crc,
            cnpj.contador.pj.id
        ) as pj
    ) as contador,
    struct(
        cnpj.responsavel.cpf,
        cnpj.responsavel.qualificacao_id,
        cnpj.responsavel.qualificacao_descricao,
        cnpj.responsavel.inclusao_data
    ) as responsavel,
    cnpj.tipos_unidade,
    cnpj.formas_atuacao,
    cnpj.socios_quantidade,
    socios_enriquecidos.socios,
    cnpj.sucessoes,
    cnpj.timestamp,
    cnpj.language,
    struct(
        cnpj.couchdb.id,
        cnpj.couchdb.key,
        cnpj.couchdb.rev,
        cnpj.couchdb.seq,
        cnpj.couchdb.last_seq
    ) as couchdb,
    struct(
        cnpj.airbyte.raw_id,
        cnpj.airbyte.extracted_at,
        cnpj.airbyte.generation_id,
        cnpj.airbyte.changes,
        cnpj.airbyte.sync_id
    ) as airbyte,
    cnpj.cnpj_particao

from {{ ref("raw_bcadastro_cnpj") }} as cnpj
left join socios_enriquecidos on cnpj.cnpj = socios_enriquecidos.cnpj