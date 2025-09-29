{{
    config(
        alias='cpf'    )
}}

select
    cpf.cpf,
    cpf.nome,
    cpf.nome_social,
    cpf.mae_nome,
    cpf.nascimento_data,
    cpf.inscricao_data,
    cpf.atualizacao_data,
    cpf.situacao_cadastral_tipo,
    cpf.sexo,
    cpf.obito_ano,
    cpf.estrangeiro_indicador,
    cpf.residente_exterior_indicador,
    struct(
        struct(
            cpf.contato.telefone.ddi,
            cpf.contato.telefone.ddd,
            cpf.contato.telefone.numero
        ) as telefone,
        cpf.contato.email
    ) as contato,
    struct(
        cpf.endereco.cep,
        cpf.endereco.uf,
        cpf.endereco.municipio,
        cpf.endereco.bairro,
        cpf.endereco.tipo_logradouro,
        cpf.endereco.logradouro,
        cpf.endereco.numero,
        cpf.endereco.complemento
    ) as endereco,
    struct(
        cpf.nascimento_local.id_pais,
        cpf.nascimento_local.pais,
        cpf.nascimento_local.uf,
        cpf.nascimento_local.id_municipio,
        cpf.nascimento_local.municipio
    ) as nascimento_local,
    struct(
        cpf.ocupacao.id,
        cpf.ocupacao.nome,
        cpf.ocupacao.id_natureza,
        cpf.ocupacao.id_ua
    ) as ocupacao,
    struct(
        cpf.metadados.ano_exercicio,
        cpf.metadados.version,
        cpf.metadados.tipo,
        cpf.metadados.timestamp
    ) as metadados,
    struct(
        cpf.airbyte.seq,
        cpf.airbyte.last_seq,
        cpf.airbyte.airbyte_raw_id,
        cpf.airbyte.airbyte_extracted_at,
        struct(
            cpf.airbyte.airbyte_meta.changes,
            cpf.airbyte.airbyte_meta.sync_id
        ) as airbyte_meta,
        cpf.airbyte.airbyte_generation_id
    ) as airbyte,
    cpf.cpf_particao

from {{ ref("raw_bcadastro_cpf") }}