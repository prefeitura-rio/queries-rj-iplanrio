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
            ) as capitalsocial,
            nullif(json_value(doc, '$.cep'), '') as cep,
            cast(
                cast(
                    nullif(json_value(doc, '$.classificacaoCrcContadorPF'), '') as int64
                ) as string
            ) as classificacaocrccontadorpf,
            cast(
                cast(
                    nullif(json_value(doc, '$.classificacaoCrcContadorPJ'), '') as int64
                ) as string
            ) as classificacaocrccontadorpj,
            nullif(json_value(doc, '$.cnae'), '') as cnae,
            nullif(json_value(doc, '$.cnaeFiscal'), '') as cnaefiscal,
            nullif(json_value(doc, '$.cnaeSecundarias'), '') as cnaesecundarias,
            nullif(json_value(doc, '$.cnpj'), '') as cnpj,
            nullif(json_value(doc, '$.cnpjSucedida'), '') as cnpjsucedida,
            nullif(json_value(doc, '$.codMunDomic'), '') as codmundomic,
            nullif(json_value(doc, '$.codMunNat'), '') as codmunnat,
            nullif(json_value(doc, '$.codNatOcup'), '') as codnatocup,
            nullif(json_value(doc, '$.codOcup'), '') as codocup,
            nullif(json_value(doc, '$.codPaisNac'), '') as codpaisnac,
            nullif(json_value(doc, '$.codPaisRes'), '') as codpaisres,
            nullif(json_value(doc, '$.codSexo'), '') as codsexo,
            nullif(json_value(doc, '$.codSitCad'), '') as codsitcad,
            nullif(json_value(doc, '$.codUA'), '') as codua,
            nullif(json_value(doc, '$.codigoMunicipio'), '') as codigomunicipio,
            nullif(json_value(doc, '$.codigoPais'), '') as codigopais,
            nullif(json_value(doc, '$.complemento'), '') as complemento,
            nullif(json_value(doc, '$.contadorPF'), '') as contadorpf,
            nullif(json_value(doc, '$.contadorPJ'), '') as contadorpj,
            nullif(json_value(doc, '$.cpfResponsavel'), '') as cpfresponsavel,
            nullif(json_value(doc, '$.dddTelefone1'), '') as dddtelefone1,
            nullif(json_value(doc, '$.dddTelefone2'), '') as dddtelefone2,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataInclusaoResponsavel'), '')
            ) as datainclusaoresponsavel,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataInicioAtividade'), '')
            ) as datainicioatividade,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataSituacaoCadastral'), '')
            ) as datasituacaocadastral,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dataSituacaoEspecial'), '')
            ) as datasituacaoespecial,
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
            end as entefederativo,
            json_extract_array(doc, '$.formasAtuacao') as formasatuacao,
            cast(
                cast(
                    nullif(json_value(doc, '$.indicadorMatriz'), '') as int64
                ) as string
            ) as indicadormatriz,
            nullif(json_value(doc, '$.language'), '') as language,
            nullif(json_value(doc, '$.logradouro'), '') as logradouro,
            cast(
                cast(nullif(json_value(doc, '$.motivoSituacao'), '') as int64) as string
            ) as motivosituacao,
            cast(
                cast(
                    nullif(json_value(doc, '$.naturezaJuridica'), '') as int64
                ) as string
            ) as naturezajuridica,
            nullif(json_value(doc, '$.nire'), '') as nire,
            nullif(json_value(doc, '$.nomeCidadeExterior'), '') as nomecidadeexterior,
            nullif(json_value(doc, '$.nomeEmpresarial'), '') as nomeempresarial,
            nullif(json_value(doc, '$.nomeFantasia'), '') as nomefantasia,
            nullif(json_value(doc, '$.numero'), '') as numero,
            cast(
                cast(nullif(json_value(doc, '$.porteEmpresa'), '') as int64) as string
            ) as porteempresa,
            cast(
                cast(
                    nullif(json_value(doc, '$.qualificacaoResponsavel'), '') as int64
                ) as string
            ) as qualificacaoresponsavel,
            nullif(
                json_value(doc, '$.sequencialCrcContadorPF'), ''
            ) as sequencialcrccontadorpf,
            nullif(
                json_value(doc, '$.sequencialCrcContadorPJ'), ''
            ) as sequencialcrccontadorpj,
            cast(
                cast(
                    nullif(json_value(doc, '$.situacaoCadastral'), '') as int64
                ) as string
            ) as situacaocadastral,
            nullif(json_value(doc, '$.situacaoEspecial'), '') as situacaoespecial,
            json_extract_array(doc, '$.socios') as socios,
            json_extract_array(doc, '$.sucessoes') as sucessoes,
            nullif(json_value(doc, '$.telefone1'), '') as telefone1,
            nullif(json_value(doc, '$.telefone2'), '') as telefone2,
            nullif(json_value(doc, '$.timestamp'), '') as timestamp,
            nullif(json_value(doc, '$.tipoLogradouro'), '') as tipologradouro,
            nullif(json_value(doc, '$.tipoOrgaoRegistro'), '') as tipoorgaoregistro,
            json_extract_array(doc, '$.tiposUnidade') as tiposunidade,
            nullif(json_value(doc, '$.tipoCrcContadorPF'), '') as tipocrccontadorpf,
            nullif(json_value(doc, '$.tipoCrcContadorPJ'), '') as tipocrccontadorpj,
            nullif(json_value(doc, '$.uf'), '') as uf,
            nullif(json_value(doc, '$.ufCrcContadorPF'), '') as ufcrccontadorpf,
            nullif(json_value(doc, '$.ufCrcContadorPJ'), '') as ufcrccontadorpj,
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

    -- separa as collections em tabelas diferentes
    matriz as (
        select * except (cnpj), cnpj as cnpj_matriz
        from fonte_parseada
        where length(cnpj) = 8
    ),
    estabelecimento as (
        select *, left(cnpj, 8) as cnpj_matriz
        from fonte_parseada
        where length(cnpj) = 14
    ),
    sucessao as (
        select * from fonte_parseada where cnpj is null and cnpjsucedida is not null
    ),

    -- dedup das tabelas
    dedup_matriz as (
        select *
        from matriz
        qualify
            row_number() over (
                partition by cnpj_matriz order by airbyte.extracted_at desc
            )
            = 1
    ),

    dedup_estabelecimento as (
        select *
        from estabelecimento
        qualify
            row_number() over (
                partition by cnpj order by airbyte.extracted_at desc
            )
            = 1
    ),

    dedup_sucessao as (
        select *
        from sucessao
        qualify
            row_number() over (
                partition by cnpjSucedida order by airbyte.extracted_at desc
            )
            = 1
    ),

    merged as (
        select
            est.cnpj,
            -- mat.cnpj_8,
            coalesce(est.bairro, mat.bairro) as bairro,
            coalesce(est.capitalsocial, mat.capitalsocial) as capitalsocial,
            coalesce(est.cep, mat.cep) as cep,
            coalesce(
                est.classificacaocrccontadorpf, mat.classificacaocrccontadorpf
            ) as classificacaocrccontadorpf,
            coalesce(
                est.classificacaocrccontadorpj, mat.classificacaocrccontadorpj
            ) as classificacaocrccontadorpj,
            coalesce(est.cnae, mat.cnae) as cnae,
            coalesce(est.cnaefiscal, mat.cnaefiscal) as cnaefiscal,
            coalesce(est.cnaesecundarias, mat.cnaesecundarias) as cnaesecundarias,
            coalesce(est.cnpjsucedida, mat.cnpjsucedida) as cnpjsucedida,
            coalesce(est.codmundomic, mat.codmundomic) as codmundomic,
            coalesce(est.codmunnat, mat.codmunnat) as codmunnat,
            coalesce(est.codnatocup, mat.codnatocup) as codnatocup,
            coalesce(est.codocup, mat.codocup) as codocup,
            coalesce(est.codpaisnac, mat.codpaisnac) as codpaisnac,
            coalesce(est.codpaisres, mat.codpaisres) as codpaisres,
            coalesce(est.codsexo, mat.codsexo) as codsexo,
            coalesce(est.codsitcad, mat.codsitcad) as codsitcad,
            coalesce(est.codua, mat.codua) as codua,
            coalesce(est.codigomunicipio, mat.codigomunicipio) as codigomunicipio,
            coalesce(est.codigopais, mat.codigopais) as codigopais,
            coalesce(est.complemento, mat.complemento) as complemento,
            coalesce(est.contadorpf, mat.contadorpf) as contadorpf,
            coalesce(est.contadorpj, mat.contadorpj) as contadorpj,
            coalesce(est.cpfresponsavel, mat.cpfresponsavel) as cpfresponsavel,
            coalesce(est.dddtelefone1, mat.dddtelefone1) as dddtelefone1,
            coalesce(est.dddtelefone2, mat.dddtelefone2) as dddtelefone2,
            coalesce(
                est.datainclusaoresponsavel, mat.datainclusaoresponsavel
            ) as datainclusaoresponsavel,
            coalesce(
                est.datainicioatividade, mat.datainicioatividade
            ) as datainicioatividade,
            coalesce(
                est.datasituacaocadastral, mat.datasituacaocadastral
            ) as datasituacaocadastral,
            coalesce(
                est.datasituacaoespecial, mat.datasituacaoespecial
            ) as datasituacaoespecial,
            coalesce(est.email, mat.email) as email,
            coalesce(est.entefederativo, mat.entefederativo) as entefederativo,
            coalesce(est.formasatuacao, mat.formasatuacao) as formasatuacao,
            coalesce(est.indicadormatriz, mat.indicadormatriz) as indicadormatriz,
            coalesce(est.language, mat.language) as language,
            coalesce(est.logradouro, mat.logradouro) as logradouro,
            coalesce(est.motivosituacao, mat.motivosituacao) as motivosituacao,
            coalesce(est.naturezajuridica, mat.naturezajuridica) as naturezajuridica,
            coalesce(est.nire, mat.nire) as nire,
            coalesce(
                est.nomecidadeexterior, mat.nomecidadeexterior
            ) as nomecidadeexterior,
            coalesce(est.nomeempresarial, mat.nomeempresarial) as nomeempresarial,
            coalesce(est.nomefantasia, mat.nomefantasia) as nomefantasia,
            coalesce(est.numero, mat.numero) as numero,
            coalesce(est.porteempresa, mat.porteempresa) as porteempresa,
            coalesce(
                est.qualificacaoresponsavel, mat.qualificacaoresponsavel
            ) as qualificacaoresponsavel,
            coalesce(
                est.sequencialcrccontadorpf, mat.sequencialcrccontadorpf
            ) as sequencialcrccontadorpf,
            coalesce(
                est.sequencialcrccontadorpj, mat.sequencialcrccontadorpj
            ) as sequencialcrccontadorpj,
            coalesce(est.situacaocadastral, mat.situacaocadastral) as situacaocadastral,
            coalesce(est.situacaoespecial, mat.situacaoespecial) as situacaoespecial,
            coalesce(est.socios, mat.socios) as socios,
            suc.sucessoes as sucessoes,
            coalesce(est.telefone1, mat.telefone1) as telefone1,
            coalesce(est.telefone2, mat.telefone2) as telefone2,
            coalesce(est.timestamp, mat.timestamp) as timestamp,
            coalesce(est.tipologradouro, mat.tipologradouro) as tipologradouro,
            coalesce(est.tipoorgaoregistro, mat.tipoorgaoregistro) as tipoorgaoregistro,
            coalesce(est.tiposunidade, mat.tiposunidade) as tiposunidade,
            coalesce(est.tipocrccontadorpf, mat.tipocrccontadorpf) as tipocrccontadorpf,
            coalesce(est.tipocrccontadorpj, mat.tipocrccontadorpj) as tipocrccontadorpj,
            coalesce(est.uf, mat.uf) as uf,
            coalesce(est.ufcrccontadorpf, mat.ufcrccontadorpf) as ufcrccontadorpf,
            coalesce(est.ufcrccontadorpj, mat.ufcrccontadorpj) as ufcrccontadorpj,
            coalesce(est.version, mat.version) as version,

            -- Airbyte fields
            struct(
                coalesce(est.airbyte.raw_id, mat.airbyte.raw_id) as raw_id,
                coalesce(
                    est.airbyte.extracted_at, mat.airbyte.extracted_at
                ) as extracted_at,
                coalesce(
                    est.airbyte.generation_id, mat.airbyte.generation_id
                ) as generation_id,
                coalesce(est.airbyte.changes, mat.airbyte.changes) as changes,
                coalesce(est.airbyte.sync_id, mat.airbyte.sync_id) as sync_id
            ) as airbyte,

            cast(est.cnpj as int64) as cnpj_particao,

        from dedup_estabelecimento as est
        left join dedup_matriz as mat on est.cnpj_matriz = mat.cnpj_matriz
        left join dedup_sucessao as suc on est.cnpj = suc.cnpjSucedida
    )

select *
from merged
