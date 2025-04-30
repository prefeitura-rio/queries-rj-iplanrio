-- tabela de CAEPF - Cadastro de Atividade Econômica da Pessoa Física
{{
    config(
        alias="chcaepf_bcadastros_parsed",
        schema="brutos_bcadastro_staging",
        materialized="table",
    )
}}


with
    fonte as (
        select *
        from {{ source("brutos_bcadastro_staging", "chcaepf_bcadastros") }}

        {% if target.name == "dev" %}
            where
                timestamp(_airbyte_extracted_at)
                >= timestamp_sub(current_timestamp(), interval 3 day)
        {% endif %}
    ),

    fonte_parseada as (
        select
            -- Campos extraídos do JSON conforme a imagem fornecida
            nullif(json_value(doc, '$.bairro'), '') as bairro,
            nullif(json_value(doc, '$.cei'), '') as cei,
            nullif(json_value(doc, '$.cep'), '') as cep,
            nullif(json_value(doc, '$.cnaes'), '') as cnaes,
            nullif(json_value(doc, '$.codAtividade'), '') as codAtividade,
            nullif(json_value(doc, '$.codMotivoSituacao'), '') as codMotivoSituacao,
            nullif(json_value(doc, '$.codMunicipio'), '') as codMunicipio,
            nullif(json_value(doc, '$.codOrgaoCpf'), '') as codOrgaoCpf,
            nullif(json_value(doc, '$.codOrgaoMunicipio'), '') as codOrgaoMunicipio,
            nullif(json_value(doc, '$.codQualificacao'), '') as codQualificacao,
            nullif(json_value(doc, '$.codSituacao'), '') as codSituacao,
            nullif(json_value(doc, '$.codSituacaoCpf'), '') as codSituacaoCpf,
            nullif(json_value(doc, '$.codTipoContribuinte'), '') as codTipoContribuinte,
            nullif(json_value(doc, '$.complemento'), '') as complemento,
            safe.parse_date(
                '%Y-%m-%d', nullif(json_value(doc, '$.dataInicioAtividade'), '')
            ) as dataInicioAtividade,
            safe.parse_date(
                '%Y-%m-%d', nullif(json_value(doc, '$.dataSituacao'), '')
            ) as dataSituacao,
            nullif(json_value(doc, '$.emailContato'), '') as emailContato,
            nullif(json_value(doc, '$.language'), '') as language,
            nullif(json_value(doc, '$.logradouro'), '') as logradouro,
            nullif(json_value(doc, '$.nomeAtividade'), '') as nomeAtividade,
            nullif(json_value(doc, '$.nomeCpf'), '') as nomeCpf,
            nullif(json_value(doc, '$.nomeMotivoSituacao'), '') as nomeMotivoSituacao,
            nullif(json_value(doc, '$.nomeMunicipio'), '') as nomeMunicipio,
            nullif(json_value(doc, '$.nomeQualificacao'), '') as nomeQualificacao,
            nullif(json_value(doc, '$.nomeSituacao'), '') as nomeSituacao,
            nullif(json_value(doc, '$.nomeSituacaoCpf'), '') as nomeSituacaoCpf,
            nullif(json_value(doc, '$.nomeTipoContribuinte'), '') as nomeTipoContribuinte,
            nullif(json_value(doc, '$.nroAepf'), '') as nroAepf,
            nullif(json_value(doc, '$.nroAepfCompleto'), '') as nroAepfCompleto,
            nullif(json_value(doc, '$.nroCpf'), '') as nroCpf,
            nullif(json_value(doc, '$.numeroLogradouro'), '') as numeroLogradouro,
            nullif(json_value(doc, '$.telefoneContato'), '') as telefoneContato,
            nullif(json_value(doc, '$.timestamp'), '') as timestamp,
            nullif(json_value(doc, '$.tipo'), '') as tipo,
            nullif(json_value(doc, '$.ufMunicipio'), '') as ufMunicipio,
            nullif(json_value(doc, '$.version'), '') as version,
            nullif(json_value(doc, '$._id'), '') as _id,
            nullif(json_value(doc, '$._rev'), '') as _rev,

            -- Airbyte fields
            struct(
                _airbyte_raw_id as raw_id,
                _airbyte_extracted_at as extracted_at,
                _airbyte_generation_id as generation_id,
                nullif(json_value(_airbyte_meta, '$.changes'), "") as changes,
                nullif(json_value(_airbyte_meta, '$.sync_id'), "") as sync_id
            ) as airbyte,
        from fonte
    ),

    dedup as (
        select *
        from fonte_parseada
        qualify
            row_number() over (partition by nroCpf order by airbyte.extracted_at desc)
            = 1
    )

select *
from dedup
