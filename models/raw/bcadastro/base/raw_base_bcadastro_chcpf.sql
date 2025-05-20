-- tabela de CPF - Cadastro de Pessoas Físicas

{{
    config(
        alias="chcpf_bcadastros_parsed",
        schema="brutos_bcadastro_staging",
        materialized="table",
    )
}}


with
    fonte as (
        select *
        from {{ source("brutos_bcadastro_staging", "chcpf_bcadastros") }}

        {% if target.name == "dev" %}
            where
                timestamp(_airbyte_extracted_at)
                >= timestamp_sub(current_timestamp(), interval 3 day)
        {% endif %}
    ),

    fonte_parseada as (
        select
            -- Alphabetically ordered fields
            cast(nullif(json_value(doc, '$.anoExerc'), "") as int64) as anoExerc,
            nullif(json_value(doc, '$.anoObito'), "") as anoObito,
            nullif(json_value(doc, '$.bairro'), "") as bairro,
            nullif(json_value(doc, '$.cep'), "") as cep,
            nullif(json_value(doc, '$.codMunDomic'), "") as codMunDomic,
            nullif(json_value(doc, '$.codMunNat'), "") as codMunNat,
            nullif(json_value(doc, '$.codNatOcup'), "") as codNatOcup,
            nullif(json_value(doc, '$.codOcup'), "") as codOcup,
            nullif(json_value(doc, '$.codPaisNac'), "") as codPaisNac,
            nullif(json_value(doc, '$.codPaisRes'), "") as codPaisRes,
            nullif(json_value(doc, '$.codSexo'), "") as codSexo,
            nullif(json_value(doc, '$.codSitCad'), "") as codSitCad,
            nullif(json_value(doc, '$.codUA'), "") as codUA,
            nullif(json_value(doc, '$.complemento'), "") as complemento,
            nullif(json_value(doc, '$.cpfId'), "") as cpfId,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dtInscricao'), "")
            ) as dtInscricao,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dtNasc'), "")
            ) as dtNasc,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dtUltAtualiz'), "")
            ) as dtUltAtualiz,
            nullif(json_value(doc, '$.email'), "") as email,
            nullif(json_value(doc, '$.indEstrangeiro'), "") as indEstrangeiro,
            nullif(json_value(doc, '$.indResExt'), "") as indResExt,
            nullif(json_value(doc, '$.logradouro'), "") as logradouro,
            nullif(json_value(doc, '$.municipio'), "") as municipio,
            nullif(json_value(doc, '$.nomeContribuinte'), "") as nomeContribuinte,
            nullif(json_value(doc, '$.nomeMae'), "") as nomeMae,
            nullif(json_value(doc, '$.nomePaisNac'), "") as nomePaisNac,
            nullif(json_value(doc, '$.nomePaisRes'), "") as nomePaisRes,
            nullif(json_value(doc, '$.nomeSocial'), "") as nomeSocial,
            nullif(json_value(doc, '$.nroLogradouro'), "") as nroLogradouro,
            nullif(json_value(doc, '$.telefone'), "") as telefone,
            nullif(json_value(doc, '$.timestamp'), "") as timestamp,
            nullif(json_value(doc, '$.tipo'), "") as tipo,
            nullif(json_value(doc, '$.tipoLogradouro'), "") as tipoLogradouro,
            nullif(json_value(doc, '$.ufMunDomic'), "") as ufMunDomic,
            nullif(json_value(doc, '$.ufMunNat'), "") as ufMunNat,
            nullif(
                json_value(replace(to_json_string(doc), '~', ''), '$.version'), ""
            ) as version,

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
            row_number() over (partition by cpfId order by airbyte.extracted_at desc)
            = 1
    )

select *
from dedup
where cpfId is not null and cpfId != ''

