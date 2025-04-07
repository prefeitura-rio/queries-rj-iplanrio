{{
    config(
        alias="cpf",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    fonte as (
        select * from {{ source("brutos_bcadastro_staging", "chcpf_bcadastros") }}

        {% if target.name == "dev" %}
            where
                timestamp(_airbyte_extracted_at)
                >= timestamp_sub(current_timestamp(), interval 3 day)
        {% endif %}
    ),

    municipio_bd as (
        select id_municipio_rf, nome as municipio_nome
        from {{ source("br_bd_diretorios_brasil", "municipio") }}
    ),

    dominio as (
        select id, {{ proper_br("descricao") }} as descricao, column
        from {{ source("brutos_bcadastro_staging","dominio_cpf") }}
    ),

    fonte_parseada as (
        select
            -- Primary key
            nullif(json_value(doc, '$.cpfId'), "") as cpf_id,

            -- Foreign keys
            nullif(json_value(doc, '$.codMunDomic'), "") as id_municipio_domicilio,
            nullif(json_value(doc, '$.codMunNat'), "") as id_nascimento_municipio,
            nullif(json_value(doc, '$.codPaisNac'), "") as id_pais_nascimento,
            nullif(json_value(doc, '$.codPaisRes'), "") as id_pais_residencia,
            nullif(json_value(doc, '$.codNatOcup'), "") as id_natureza_ocupacao,
            nullif(json_value(doc, '$.codOcup'), "") as id_ocupacao,
            nullif(json_value(doc, '$.codUA'), "") as id_ua,

            -- Person data
            nullif(json_value(doc, '$.nomeContribuinte'), "") as nome,
            nullif(json_value(doc, '$.nomeSocial'), "") as nome_social,
            nullif(json_value(doc, '$.nomeMae'), "") as mae_nome,

            -- Dates
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dtNasc'), "")
            ) as nascimento_data,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dtInscricao'), "")
            ) as inscricao_data,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(doc, '$.dtUltAtualiz'), "")
            ) as atualizacao_data,

            -- Status and demographics
            nullif(json_value(doc, '$.codSitCad'), "") as id_situacao_cadastral,
            nullif(json_value(doc, '$.codSexo'), "") as id_sexo,
            nullif(json_value(doc, '$.anoObito'), "") as obito_ano,
            nullif(json_value(doc, '$.indEstrangeiro'), "") as indicativo_estrangeiro,
            nullif(json_value(doc, '$.indResExt'), "") as indicativo_residente_exterior,

            -- Contact
            nullif(json_value(doc, '$.telefone'), "") as telefone,  -- necessário tratativa para extrair ddi,ddd,numero
            nullif(json_value(doc, '$.email'), "") as email,

            -- Address
            nullif(json_value(doc, '$.cep'), "") as endereco_cep,
            nullif(json_value(doc, '$.ufMunDomic'), "") as endereco_uf,
            nullif(json_value(doc, '$.bairro'), "") as endereco_bairro,
            nullif(json_value(doc, '$.tipoLogradouro'), "") as endereco_tipo_logradouro,
            nullif(json_value(doc, '$.logradouro'), "") as endereco_logradouro,
            nullif(json_value(doc, '$.nroLogradouro'), "") as endereco_numero,
            nullif(json_value(doc, '$.complemento'), "") as endereco_complemento,
            nullif(json_value(doc, '$.municipio'), "") as endereco_municipio,

            -- Birth and residence
            nullif(json_value(doc, '$.ufMunNat'), "") as nascimento_uf,
            nullif(json_value(doc, '$.nomePaisNac'), "") as nascimento_pais,
            nullif(json_value(doc, '$.nomePaisRes'), "") as residencia_pais,

            -- Metadata
            cast(nullif(json_value(doc, '$.anoExerc'), "") as int64) as exercicio_ano,
            nullif(
                json_value(replace(to_json_string(doc), '~', ''), '$.version'), ""
            ) as version,
            nullif(json_value(doc, '$.tipo'), "") as tipo,
            nullif(json_value(doc, '$.timestamp'), "") as timestamp,

            -- Technical fields
            seq,
            last_seq,
            nullif(json_value(doc, '$.id'), "") as id_doc,
            id,
            key,
            nullif(json_value(value, '$.rev'), "") as rev,

            nullif(json_value(doc, '$._id'), "") as _id,
            nullif(json_value(doc, '$._rev'), "") as _rev,
            _airbyte_raw_id as airbyte_raw_id,
            _airbyte_extracted_at as airbyte_extracted_at,
            struct(
                nullif(json_value(_airbyte_meta, '$.changes'), "") as changes,
                nullif(json_value(_airbyte_meta, '$.sync_id'), "") as sync_id
            ) as airbyte_meta,
            _airbyte_generation_id as airbyte_generation_id
        from fonte
    ),

    fonte_intermediaria as (
        select
            -- Primary key
            cpf_id as cpf,

            -- Foreign keys
            id_municipio_domicilio,
            id_nascimento_municipio,
            id_pais_nascimento,
            id_pais_residencia,
            id_natureza_ocupacao,
            t.id_ocupacao,
            id_ua,

            -- Person data
            nome,
            nome_social,
            mae_nome,

            -- Dates
            nascimento_data,
            inscricao_data,
            atualizacao_data,

            -- Status and demographics
            sc.descricao as situacao_cadastral_tipo,
            s.descricao as sexo,
            obito_ano,
            ie.descricao as estrangeiro_indicador,
            re.descricao as residente_exterior_indicador,

            -- Contact
            telefone,  -- necessário tratativa para extrair ddi,ddd,numero
            email,

            -- Address
            endereco_cep,
            endereco_uf,
            endereco_bairro,
            endereco_tipo_logradouro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,

            -- Birth and residence
            nascimento_uf,
            mn.municipio_nome as nascimento_municipio,
            nascimento_pais,
            residencia_pais,
            md.municipio_nome as endereco_municipio,

            -- Occupation
            o.descricao as ocupacao_nome,

            -- Metadata
            exercicio_ano,
            version,
            tipo,
            timestamp,

            -- Technical fields
            seq,
            last_seq,
            airbyte_raw_id,
            airbyte_extracted_at,
            airbyte_meta,
            airbyte_generation_id,

            -- Partition
            cast(cpf_id as int64) as cpf_particao,

            -- Outros
            id,
            _id,
            key,
            rev,
            _rev,
            id_doc,

        from fonte_parseada t
        left join
            municipio_bd as md
            on cast(t.id_municipio_domicilio as int64)
            = cast(md.id_municipio_rf as int64)
        left join
            municipio_bd as mn
            on cast(t.id_municipio_domicilio as int64)
            = cast(mn.id_municipio_rf as int64)
        left join
            (
                select id as id_ocupacao, descricao
                from dominio
                where column = 'ocupacao'
            ) o
            on t.id_ocupacao = o.id_ocupacao
        left join
            (select id as id_sexo, descricao from dominio where column = 'sexo') s
            on t.id_sexo = s.id_sexo
        left join
            (
                select id as id_situacao_cadastral, descricao
                from dominio
                where column = 'situacao_cadastral'
            ) sc
            on t.id_situacao_cadastral = sc.id_situacao_cadastral
        left join
            (
                select
                    id as indicativo_estrangeiro,
                    case
                        upper(id) when 'N' then false when 'S' then true else null
                    end as descricao
                from dominio
                where column = 'indicativo_estrangeiro'
            ) ie
            on upper(t.indicativo_estrangeiro) = upper(ie.indicativo_estrangeiro)
        left join
            (
                select
                    id as indicativo_residente_exterior,
                    case
                        upper(id) when 'N' then false when 'S' then true else null
                    end as descricao
                from dominio
                where column = 'indicativo_residente_exterior'
            ) re
            on upper(t.indicativo_residente_exterior)
            = upper(re.indicativo_residente_exterior)
    ),

    fonte_padronizada as (
        select
            -- Primary key
            cpf,

            -- Foreign keys
            id_municipio_domicilio,
            id_nascimento_municipio,
            id_pais_nascimento,
            id_pais_residencia,
            id_natureza_ocupacao,
            id_ocupacao,
            id_ua,

            -- Person data
            {{ proper_br("nome") }} as nome,
            nome_social,
            {{ proper_br("mae_nome") }} as mae_nome,

            -- Dates
            nascimento_data,
            inscricao_data,
            atualizacao_data,

            -- Status and demographics
            {{ proper_br("situacao_cadastral_tipo") }} as situacao_cadastral_tipo,
            lower(sexo) as sexo,
            obito_ano,
            estrangeiro_indicador,
            residente_exterior_indicador,

            -- Contact
            TRIM(telefone) as telefone_original,
            case
                when regexp_contains(telefone, r'\+')
                then regexp_extract(telefone, r'\+([^\s]+)')
                else null
            end as telefone_ddi,
            case
                when regexp_contains(telefone, r'\(')
                then regexp_extract(telefone, r'\(([^\)]+)\)')
                else null
            end as telefone_ddd,
            case
                when telefone is not null
                then
                    if(
                        strpos(reverse(regexp_replace(telefone, r'-', '')), ' ') > 0,
                        substr(
                            regexp_replace(telefone, r'-', ''),
                            length(telefone) - strpos(reverse(telefone), ' ') + 1
                        ),
                        regexp_replace(telefone, r'-', '')
                    )
                else null
            end as telefone_numero,
            email,

            -- Address
            endereco_cep,
            lower(endereco_uf) as endereco_uf,
            {{ proper_br("endereco_municipio") }} as endereco_municipio,
            {{ proper_br("endereco_bairro") }} as endereco_bairro,
            {{ proper_br("endereco_tipo_logradouro") }} as endereco_tipo_logradouro,
            {{ proper_br("endereco_logradouro") }} as endereco_logradouro,
            endereco_numero,
            {{ proper_br("endereco_complemento") }} as endereco_complemento,

            -- Birth and residence
            lower(nascimento_uf) as nascimento_uf,
            {{ proper_br("nascimento_municipio") }} as nascimento_municipio,
            nascimento_pais,
            residencia_pais,

            -- Occupation
            {{ proper_br("ocupacao_nome") }} as ocupacao_nome,

            -- Metadata
            exercicio_ano as ano_exercicio,
            version,
            tipo,
            timestamp,

            -- Technical fields
            struct(
                seq,
                last_seq,
                airbyte_raw_id,
                airbyte_extracted_at,
                airbyte_meta,
                airbyte_generation_id
            ) as airbyte,

            -- Partition
            cpf_particao,

            -- Outros
            id,
            _id,
            key,
            rev,
            _rev,
            id_doc
        from fonte_intermediaria
    ),

    fonte_deduplicada as (
        select *
        from fonte_padronizada
        qualify row_number() over (partition by cpf order by atualizacao_data desc) = 1
    ),

    final as (

        select
            -- Primary key
            cpf,

            -- Foreign keys
            id_municipio_domicilio,
            id_nascimento_municipio,
            id_pais_nascimento,
            id_pais_residencia,
            id_natureza_ocupacao,
            id_ocupacao,
            id_ua,

            -- Person data
            nome,
            nome_social,
            mae_nome,

            -- Dates
            nascimento_data,
            inscricao_data,
            atualizacao_data,

            -- Status and demographics
            situacao_cadastral_tipo,
            sexo,
            obito_ano,
            estrangeiro_indicador,
            residente_exterior_indicador,

            -- Contact
            TRIM(telefone_ddi) as telefone_ddi,
            TRIM(telefone_ddd) as telefone_ddd,
            TRIM(telefone_numero) as telefone_numero,
            email,

            -- Address
            endereco_cep,
            endereco_uf,
            endereco_municipio,
            endereco_bairro,
            endereco_tipo_logradouro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,

            -- Birth and residence
            nascimento_uf,
            nascimento_municipio,
            nascimento_pais,
            residencia_pais,

            -- Occupation
            ocupacao_nome,

            -- Metadata
            ano_exercicio,
            version,
            tipo,
            timestamp,

            -- Technical fields
            airbyte,

            -- Partition
            cpf_particao
        from fonte_deduplicada
    )

select *
from final
