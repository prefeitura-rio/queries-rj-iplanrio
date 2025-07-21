-- tabela de CNPJ - Cadastro de Pessoas Jurídicas
{{
    config(
        alias="chcnpj_bcadastros_parsed",
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
                nullif(json_value(doc, '$.capitalSocial'), '') as int64
            ) as capitalSocial,
            nullif(json_value(doc, '$.cep'), '') as cep,
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
            nullif(json_value(doc, '$.cnaeFiscal'), '') as cnaefiscal,
            nullif(json_value(doc, '$.cnaeSecundarias'), '') as cnaesecundarias,
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
        select
            *,
            -- Partition by cnpj
            cast(cnpj as int64) as cnpj_particao,
        from fonte_parseada
        qualify
            row_number() over (partition by cnpj order by airbyte.extracted_at desc) = 1
    ),

    -- FIX: trucamento de registros com 8 digitos que perderam os digitos depois da barra
    --- relacao de cnpj com 14 digitos
    dedup_14 as (select *, left(cnpj, 8) as cnpj_8 from dedup where length(cnpj) = 14),

    -- relacao de cnpj com 8 digitos (perderam os digitos depois da barra)
    dedup_8 as (select *, cnpj as cnpj_8 from dedup where length(cnpj) = 8),

    -- cnpj com 8 digitos com mais de 1 registro de 14 digitos
    cnpj_em_quarentena as (
        select cnpj, cnpj_8 from dedup_14
        where cnpj_8 in (
            select cnpj_8 from dedup_14 group by cnpj_8 having count(*) > 1
        )
        order by cnpj
    ),

    merge_14_8 as (
        select
            d14.cnpj,
            -- d8.cnpj_8,
            coalesce(d14.bairro, d8.bairro) as bairro,
            coalesce(d14.capitalSocial, d8.capitalSocial) as capitalSocial,
            coalesce(d14.cep, d8.cep) as cep,
            coalesce(d14.classificacaoCrcContadorPF, d8.classificacaoCrcContadorPF) as classificacaoCrcContadorPF,
            coalesce(d14.classificacaoCrcContadorPJ, d8.classificacaoCrcContadorPJ) as classificacaoCrcContadorPJ,
            coalesce(d14.cnae, d8.cnae) as cnae,
            coalesce(d14.cnaeFiscal, d8.cnaeFiscal) as cnaeFiscal,
            coalesce(d14.cnaeSecundarias, d8.cnaeSecundarias) as cnaeSecundarias,
            coalesce(d14.cnpj_particao, d8.cnpj_particao) as cnpj_particao,
            coalesce(d14.cnpjSucedida, d8.cnpjSucedida) as cnpjSucedida,
            coalesce(d14.codMunDomic, d8.codMunDomic) as codMunDomic,
            coalesce(d14.codMunNat, d8.codMunNat) as codMunNat,
            coalesce(d14.codNatOcup, d8.codNatOcup) as codNatOcup,
            coalesce(d14.codOcup, d8.codOcup) as codOcup,
            coalesce(d14.codPaisNac, d8.codPaisNac) as codPaisNac,
            coalesce(d14.codPaisRes, d8.codPaisRes) as codPaisRes,
            coalesce(d14.codSexo, d8.codSexo) as codSexo,
            coalesce(d14.codSitCad, d8.codSitCad) as codSitCad,
            coalesce(d14.codUA, d8.codUA) as codUA,
            coalesce(d14.codigoMunicipio, d8.codigoMunicipio) as codigoMunicipio,
            coalesce(d14.codigoPais, d8.codigoPais) as codigoPais,
            coalesce(d14.complemento, d8.complemento) as complemento,
            coalesce(d14.contadorPF, d8.contadorPF) as contadorPF,
            coalesce(d14.contadorPJ, d8.contadorPJ) as contadorPJ,
            coalesce(d14.cpfResponsavel, d8.cpfResponsavel) as cpfResponsavel,
            coalesce(d14.dddTelefone1, d8.dddTelefone1) as dddTelefone1,
            coalesce(d14.dddTelefone2, d8.dddTelefone2) as dddTelefone2,
            coalesce(d14.dataInclusaoResponsavel, d8.dataInclusaoResponsavel) as dataInclusaoResponsavel,
            coalesce(d14.dataInicioAtividade, d8.dataInicioAtividade) as dataInicioAtividade,
            coalesce(d14.dataSituacaoCadastral, d8.dataSituacaoCadastral) as dataSituacaoCadastral,
            coalesce(d14.dataSituacaoEspecial, d8.dataSituacaoEspecial) as dataSituacaoEspecial,
            coalesce(d14.email, d8.email) as email,
            coalesce(d14.enteFederativo, d8.enteFederativo) as enteFederativo,
            coalesce(d14.formasAtuacao, d8.formasAtuacao) as formasAtuacao,
            coalesce(d14.indicadorMatriz, d8.indicadorMatriz) as indicadorMatriz,
            coalesce(d14.language, d8.language) as language,
            coalesce(d14.logradouro, d8.logradouro) as logradouro,
            coalesce(d14.motivoSituacao, d8.motivoSituacao) as motivoSituacao,
            coalesce(d14.naturezaJuridica, d8.naturezaJuridica) as naturezaJuridica,
            coalesce(d14.nire, d8.nire) as nire,
            coalesce(d14.nomeCidadeExterior, d8.nomeCidadeExterior) as nomeCidadeExterior,
            coalesce(d8.nomeEmpresarial, d14.nomeEmpresarial) as nomeEmpresarial,
            coalesce(d14.nomeFantasia, d8.nomeFantasia) as nomeFantasia,
            coalesce(d14.numero, d8.numero) as numero,
            coalesce(d14.porteEmpresa, d8.porteEmpresa) as porteEmpresa,
            coalesce(d14.qualificacaoResponsavel, d8.qualificacaoResponsavel) as qualificacaoResponsavel,
            coalesce(d14.sequencialCrcContadorPF, d8.sequencialCrcContadorPF) as sequencialCrcContadorPF,
            coalesce(d14.sequencialCrcContadorPJ, d8.sequencialCrcContadorPJ) as sequencialCrcContadorPJ,
            coalesce(d14.situacaoCadastral, d8.situacaoCadastral) as situacaoCadastral,
            coalesce(d14.situacaoEspecial, d8.situacaoEspecial) as situacaoEspecial,
            coalesce(d14.socios, d8.socios) as socios,
            coalesce(d14.sucessoes, d8.sucessoes) as sucessoes,
            coalesce(d14.telefone1, d8.telefone1) as telefone1,
            coalesce(d14.telefone2, d8.telefone2) as telefone2,
            coalesce(d14.timestamp, d8.timestamp) as timestamp,
            coalesce(d14.tipoLogradouro, d8.tipoLogradouro) as tipoLogradouro,
            coalesce(d14.tipoOrgaoRegistro, d8.tipoOrgaoRegistro) as tipoOrgaoRegistro,
            coalesce(d14.tiposUnidade, d8.tiposUnidade) as tiposUnidade,
            coalesce(d14.tipoCrcContadorPF, d8.tipoCrcContadorPF) as tipoCrcContadorPF,
            coalesce(d14.tipoCrcContadorPJ, d8.tipoCrcContadorPJ) as tipoCrcContadorPJ,
            coalesce(d14.uf, d8.uf) as uf,
            coalesce(d14.ufCrcContadorPF, d8.ufCrcContadorPF) as ufCrcContadorPF,
            coalesce(d14.ufCrcContadorPJ, d8.ufCrcContadorPJ) as ufCrcContadorPJ,
            coalesce(d14.version, d8.version) as version,
            
            -- Airbyte fields
            coalesce(d14.airbyte.raw_id, d8.airbyte.raw_id) as airbyte_raw_id,
            coalesce(d14.airbyte.extracted_at, d8.airbyte.extracted_at) as airbyte_extracted_at,
            coalesce(d14.airbyte.generation_id, d8.airbyte.generation_id) as airbyte_generation_id,
            coalesce(d14.airbyte.changes, d8.airbyte.changes) as airbyte_changes,
            coalesce(d14.airbyte.sync_id, d8.airbyte.sync_id) as airbyte_sync_id
            
        from dedup_14 as d14
        left join dedup_8 as d8 on d14.cnpj_8 = d8.cnpj_8
        where d14.cnpj not in (select cnpj from cnpj_em_quarentena)
    )

select * from merge_14_8
