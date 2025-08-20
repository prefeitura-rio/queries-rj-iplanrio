{{
    config(
        alias="pessoa_fisica",
        schema="rmi_dados_mestres",
        materialized="table",
        tags=["daily"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    -- SOURCE
    all_prefeitura as (select distinct cpf from {{ ref("int_pessoa_fisica_all_cpf") }}),

    source_bcadastro as (
        select *
        from (select * from {{ source("brutos_bcadastro", "cpf") }})
        inner join all_prefeitura using (cpf)
    ),
    -- TODO: trazer tudo de dim_saude
    source_saude as (
        select *
        from {{ source("rj-sms", "paciente") }}
        inner join all_prefeitura using (cpf)
    ),

    -- TODO: trazer tudo de dim_assistencia_social
    source_cadunico as (
        select cpf, dados[offset(0)] as dados  -- #TODO: corrigir no cadunico para retornar um array de dados
        from {{ source("rj-smas", "cadastros") }}
        inner join all_prefeitura using (cpf)
    ),

    -- DIMENSIONS
    -- - Informações básicas
    dim_documentos as (
        select all_prefeitura.cpf, struct(source_saude.cns) as documentos,
        from all_prefeitura
        left join source_saude using (cpf)

    ),

    dim_nascimento as (
        select
            cpf,
            struct(
                nascimento_data as data,
                nascimento_local.id_municipio as municipio_id,
                nascimento_local.municipio as municipio,
                upper(nascimento_local.uf) as uf,
                if(
                    nascimento_local.id_pais is not null,
                    nascimento_local.id_pais,
                    '105'
                ) as pais_id,
                if(
                    nascimento_local.id_pais is not null,
                    {{ proper_br("nascimento_local.pais") }},
                    'Brasil'
                ) as pais

            ) as nascimento
        from source_bcadastro
    ),

    dim_mae as (
        select
            cpf,
            struct(
                {{ proper_br("mae_nome") }} as nome, cast(null as string) as cpf
            ) as mae
        from source_bcadastro
    ),

    -- - Pontos de contato
    dim_endereco as (select * from {{ ref("int_pessoa_fisica_dim_endereco") }}),

    dim_email as (select * from {{ ref("int_pessoa_fisica_dim_email") }}),

    dim_telefone as (select * from {{ ref("int_pessoa_fisica_dim_telefone") }}),


    -- - Orgaos
    dim_assistencia_social as (select * from {{ ref("int_pessoa_fisica_dim_assistencia_social") }}),

    dim_saude as (select * from {{ ref("int_pessoa_fisica_dim_saude") }}),

    dim_ocupacao as (select * from {{ ref("int_pessoa_fisica_dim_ocupacao") }}),

    -- FINAL TABLE
    final as (
        select
            -- Primary Key
            all_prefeitura.cpf,

            -- Identificação
            {{ proper_br("bcadastro.nome") }} as nome,
            {{ proper_br("bcadastro.nome_social") }} as nome_social,
            bcadastro.sexo,
            dim_nascimento.nascimento,

            -- Parentesco
            dim_mae.mae,

            -- Outras características
            (
                date_diff(current_date(), dim_nascimento.nascimento.data, year) < 18
            ) as menor_idade,

            coalesce(
                saude.dados.raca, {{ proper_br("cadunico.dados.raca_cor") }}
            ) as raca,

            struct(
                coalesce(
                    saude.dados.obito_indicador,
                    if(bcadastro.obito_ano is not null, true, false),
                    false
                ) as indicador,
                coalesce(
                    extract(year from saude.dados.obito_data),
                    cast(bcadastro.obito_ano as int64)
                ) as ano
            ) as obito,

            -- Documentos
            dim_documentos.documentos,

            -- Endereço
            dim_endereco.endereco,

            -- Contato
            dim_email.email,
            dim_telefone.telefone,

            -- Órgão da prefeitura
            dim_assistencia_social.assistencia_social,
            dim_saude.saude,
            -- struct(dim_saude.clinica_familia, dim_saude.equipe_saude_familia) as saude,

            dim_ocupacao.ocupacao,

            -- Sócio-econômicos
            -- Participação societária
            -- Metadados e partição
            struct(current_timestamp() as last_updated) as datalake,
            cast(cpf as int64) as cpf_particao,

        from all_prefeitura
        inner join source_bcadastro as bcadastro using (cpf)
        left join dim_assistencia_social using (cpf)
        left join dim_documentos using (cpf)
        left join dim_email using (cpf)
        left join dim_endereco using (cpf)
        left join dim_mae using (cpf)
        left join dim_nascimento using (cpf)
        left join dim_saude using (cpf)
        left join dim_telefone using (cpf)
        left join dim_ocupacao using (cpf)
        left join source_saude as saude using (cpf)
        left join source_cadunico as cadunico using (cpf)


    )

select * from final

