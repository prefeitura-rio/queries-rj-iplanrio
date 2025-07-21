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
            d8.cnpj_8,
            coalesce(d14.bairro, d8.bairro) as bairro,
            coalesce(d14.capitalsocial, d8.capitalsocial) as capitalsocial,
            coalesce(d14.cep, d8.cep) as cep,
            coalesce(d14.classificacaocrccontadorpf, d8.classificacaocrccontadorpf) as classificacaocrccontadorpf,
            coalesce(d14.classificacaocrccontadorpj, d8.classificacaocrccontadorpj) as classificacaocrccontadorpj,
            coalesce(d14.cnae, d8.cnae) as cnae,
            coalesce(d14.cnaefiscal, d8.cnaefiscal) as cnaefiscal,
            coalesce(d14.cnaesecundarias, d8.cnaesecundarias) as cnaesecundarias,
            coalesce(d14.cnpj_particao, d8.cnpj_particao) as cnpj_particao,
            coalesce(d14.cnpjsucedida, d8.cnpjsucedida) as cnpjsucedida,
            coalesce(d14.codmundomic, d8.codmundomic) as codmundomic,
            coalesce(d14.codmunnat, d8.codmunnat) as codmunnat,
            coalesce(d14.codnatocup, d8.codnatocup) as codnatocup,
            coalesce(d14.codocup, d8.codocup) as codocup,
            coalesce(d14.codpaisnac, d8.codpaisnac) as codpaisnac,
            coalesce(d14.codpaisres, d8.codpaisres) as codpaisres,
            coalesce(d14.codsexo, d8.codsexo) as codsexo,
            coalesce(d14.codsitcad, d8.codsitcad) as codsitcad,
            coalesce(d14.codua, d8.codua) as codua,
            coalesce(d14.codigomunicipio, d8.codigomunicipio) as codigomunicipio,
            coalesce(d14.codigopais, d8.codigopais) as codigopais,
            coalesce(d14.complemento, d8.complemento) as complemento,
            coalesce(d14.contadorpf, d8.contadorpf) as contadorpf,
            coalesce(d14.contadorpj, d8.contadorpj) as contadorpj,
            coalesce(d14.cpfresponsavel, d8.cpfresponsavel) as cpfresponsavel,
            coalesce(d14.dddtelefone1, d8.dddtelefone1) as dddtelefone1,
            coalesce(d14.dddtelefone2, d8.dddtelefone2) as dddtelefone2,
            coalesce(d14.datainclusaoresponsavel, d8.datainclusaoresponsavel) as datainclusaoresponsavel,
            coalesce(d14.datainicioatividade, d8.datainicioatividade) as datainicioatividade,
            coalesce(d14.datasituacaocadastral, d8.datasituacaocadastral) as datasituacaocadastral,
            coalesce(d14.datasituacaoespecial, d8.datasituacaoespecial) as datasituacaoespecial,
            coalesce(d14.email, d8.email) as email,
            coalesce(d14.entefederativo, d8.entefederativo) as entefederativo,
            coalesce(d14.formasatuacao, d8.formasatuacao) as formasatuacao,
            coalesce(d14.indicadormatriz, d8.indicadormatriz) as indicadormatriz,
            coalesce(d14.language, d8.language) as language,
            coalesce(d14.logradouro, d8.logradouro) as logradouro,
            coalesce(d14.motivosituacao, d8.motivosituacao) as motivosituacao,
            coalesce(d14.naturezajuridica, d8.naturezajuridica) as naturezajuridica,
            coalesce(d14.nire, d8.nire) as nire,
            coalesce(d14.nomecidadeexterior, d8.nomecidadeexterior) as nomecidadeexterior,
            coalesce(d8.nomeempresarial, d14.nomeempresarial) as nomeempresarial,
            coalesce(d14.nomefantasia, d8.nomefantasia) as nomefantasia,
            coalesce(d14.numero, d8.numero) as numero,
            coalesce(d14.porteempresa, d8.porteempresa) as porteempresa,
            coalesce(d14.qualificacaoresponsavel, d8.qualificacaoresponsavel) as qualificacaoresponsavel,
            coalesce(d14.sequencialcrccontadorpf, d8.sequencialcrccontadorpf) as sequencialcrccontadorpf,
            coalesce(d14.sequencialcrccontadorpj, d8.sequencialcrccontadorpj) as sequencialcrccontadorpj,
            coalesce(d14.situacaocadastral, d8.situacaocadastral) as situacaocadastral,
            coalesce(d14.situacaoespecial, d8.situacaoespecial) as situacaoespecial,
            coalesce(d14.socios, d8.socios) as socios,
            coalesce(d14.sucessoes, d8.sucessoes) as sucessoes,
            coalesce(d14.telefone1, d8.telefone1) as telefone1,
            coalesce(d14.telefone2, d8.telefone2) as telefone2,
            coalesce(d14.timestamp, d8.timestamp) as timestamp,
            coalesce(d14.tipologradouro, d8.tipologradouro) as tipologradouro,
            coalesce(d14.tipoorgaoregistro, d8.tipoorgaoregistro) as tipoorgaoregistro,
            coalesce(d14.tiposunidade, d8.tiposunidade) as tiposunidade,
            coalesce(d14.tipocrccontadorpf, d8.tipocrccontadorpf) as tipocrccontadorpf,
            coalesce(d14.tipocrccontadorpj, d8.tipocrccontadorpj) as tipocrccontadorpj,
            coalesce(d14.uf, d8.uf) as uf,
            coalesce(d14.ufcrccontadorpf, d8.ufcrccontadorpf) as ufcrccontadorpf,
            coalesce(d14.ufcrccontadorpj, d8.ufcrccontadorpj) as ufcrccontadorpj,
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

