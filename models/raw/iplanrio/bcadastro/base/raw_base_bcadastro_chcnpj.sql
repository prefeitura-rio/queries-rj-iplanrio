-- tabela de CNPJ - Cadastro de Pessoas Jurídicas

{{
    config(
        alias="chcnpj_bcadastros_parsed_cnpj_new_airbyte",
        schema="brutos_bcadastro_staging",
        materialized="table",
    )
}}


with
    fonte as (
        select *
        from {{ source("brutos_bcadastro_staging", "chcnpj_bcadastros") }}

        {% if target.name == "dev" %}
            where
                timestamp(_airbyte_extracted_at)
                >= timestamp_sub(current_timestamp(), interval 3 day)
        {% endif %}
    ),

    fonte_parseada as (
        select
            -- Alphabetically ordered fields
            nullif(json_value(doc, '$.bairro'), '') as bairro,
            cast(
                cast(
                    nullif(json_value(doc, '$.classificacaoCrcContadorPF'), '') as int64
                ) as string
            ) as classificacaoCrcContadorPF,
            cast(
                cast(
                    nullif(json_value(doc, '$.classificacaoCrcContadorPJ'), '') as int64
                ) as string
            ) as classificacaoCrcContadorPJ,
            nullif(json_value(doc, '$.cnae'), '') as cnae,
            nullif(json_value(doc, '$.cnaeSecundarias'), '') as cnaeSecundarias,
            nullif(json_value(doc, '$.cnpj'), '') as cnpj,
            nullif(json_value(doc, '$.cnpjSucedida'), '') as cnpjSucedida,
            nullif(json_value(doc, '$.codMunDomic'), '') as codMunDomic,
            nullif(json_value(doc, '$.codMunNat'), '') as codMunNat,
            nullif(json_value(doc, '$.codNatOcup'), '') as codNatOcup,
            nullif(json_value(doc, '$.codOcup'), '') as codOcup,
            nullif(json_value(doc, '$.codPaisNac'), '') as codPaisNac,
            nullif(json_value(doc, '$.codPaisRes'), '') as codPaisRes,
            nullif(json_value(doc, '$.codSexo'), '') as codSexo,
            nullif(json_value(doc, '$.codSitCad'), '') as codSitCad,
            nullif(json_value(doc, '$.codUA'), '') as codUA,
            nullif(json_value(doc, '$.codigoMunicipio'), '') as codigoMunicipio,
            nullif(json_value(doc, '$.codigoPais'), '') as codigoPais,
            nullif(json_value(doc, '$.complemento'), '') as complemento,
            nullif(json_value(doc, '$.contadorPF'), '') as contadorPF,
            nullif(json_value(doc, '$.contadorPJ'), '') as contadorPJ,
            nullif(json_value(doc, '$.cpfResponsavel'), '') as cpfResponsavel,
            nullif(json_value(doc, '$.dddTelefone1'), '') as dddTelefone1,
            nullif(json_value(doc, '$.dddTelefone2'), '') as dddTelefone2,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataInclusaoResponsavel'), '')
            ) as dataInclusaoResponsavel,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataInicioAtividade'), '')
            ) as dataInicioAtividade,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataSituacaoCadastral'), '')
            ) as dataSituacaoCadastral,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataSituacaoEspecial'), '')
            ) as dataSituacaoEspecial,
            nullif(json_value(doc, '$.email'), '') as email,
            case
                when regexp_contains(json_value(doc, '$.enteFederativo'), r'^[0-9]+$')
                then
                    cast(
                        cast(
                            nullif(json_value(doc, '$.enteFederativo'), '') as int64
                        ) as string
                    )
                else upper(nullif(json_value(doc, '$.enteFederativo'), ''))
            end as enteFederativo,
            json_extract_array(doc, '$.formasAtuacao') as formasAtuacao,
            cast(
                cast(
                    nullif(json_value(doc, '$.indicadorMatriz'), '') as int64
                ) as string
            ) as indicadorMatriz,
            nullif(json_value(doc, '$.language'), '') as language,
            nullif(json_value(doc, '$.logradouro'), '') as logradouro,
            cast(
                cast(nullif(json_value(doc, '$.motivoSituacao'), '') as int64) as string
            ) as motivoSituacao,
            cast(
                cast(
                    nullif(json_value(doc, '$.naturezaJuridica'), '') as int64
                ) as string
            ) as naturezaJuridica,
            nullif(json_value(doc, '$.nire'), '') as nire,
            nullif(json_value(doc, '$.nomeCidadeExterior'), '') as nomeCidadeExterior,
            nullif(json_value(doc, '$.nomeEmpresarial'), '') as nomeEmpresarial,
            nullif(json_value(doc, '$.nomeFantasia'), '') as nomeFantasia,
            nullif(json_value(doc, '$.numero'), '') as numero,
            cast(
                cast(nullif(json_value(doc, '$.porteEmpresa'), '') as int64) as string
            ) as porteEmpresa,
            cast(
                cast(
                    nullif(json_value(doc, '$.qualificacaoResponsavel'), '') as int64
                ) as string
            ) as qualificacaoResponsavel,
            nullif(
                json_value(doc, '$.sequencialCrcContadorPF'), ''
            ) as sequencialCrcContadorPF,
            nullif(
                json_value(doc, '$.sequencialCrcContadorPJ'), ''
            ) as sequencialCrcContadorPJ,
            cast(
                cast(
                    nullif(json_value(doc, '$.situacaoCadastral'), '') as int64
                ) as string
            ) as situacaoCadastral,
            nullif(json_value(doc, '$.situacaoEspecial'), '') as situacaoEspecial,
            json_extract_array(doc, '$.socios') as socios,
            json_extract_array(doc, '$.sucessoes') as sucessoes,
            nullif(json_value(doc, '$.telefone1'), '') as telefone1,
            nullif(json_value(doc, '$.telefone2'), '') as telefone2,
            nullif(json_value(doc, '$.timestamp'), '') as timestamp,
            nullif(json_value(doc, '$.tipoLogradouro'), '') as tipoLogradouro,
            nullif(json_value(doc, '$.tipoOrgaoRegistro'), '') as tipoOrgaoRegistro,
            json_extract_array(doc, '$.tiposUnidade') as tiposUnidade,
            nullif(json_value(doc, '$.tipoCrcContadorPF'), '') as tipoCrcContadorPF,
            nullif(json_value(doc, '$.tipoCrcContadorPJ'), '') as tipoCrcContadorPJ,
            nullif(json_value(doc, '$.uf'), '') as uf,
            nullif(json_value(doc, '$.ufCrcContadorPF'), '') as ufCrcContadorPF,
            nullif(json_value(doc, '$.ufCrcContadorPJ'), '') as ufCrcContadorPJ,
            nullif(
                json_value(replace(to_json_string(doc), '~', ''), '$.version'), ''
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
        select *,
        -- Partition by cnpj
            cast(cnpj as int64) as cnpj_particao,
        from fonte_parseada
        qualify
            row_number() over (partition by cnpj order by airbyte.extracted_at desc)
            = 1
    )

select *
from dedup