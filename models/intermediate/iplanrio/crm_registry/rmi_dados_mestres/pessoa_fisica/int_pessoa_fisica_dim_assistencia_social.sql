-- Consolida informações de assistência social (CRAS) de pessoa física a partir do
-- sistema SMAS
-- TODO: Ajustar nomes dos campos conforme o schema real da tabela 'cadastros' do
-- rj-smas
{{
    config(
        alias="dim_assistencia_social",
        schema="intermediario_dados_mestres",
        materialized=("table" if target.name == "dev" else "ephemeral"),
    )
}}

with
    -- Fonte de CPFs
    all_cpf as (select cpf, cpf_particao from {{ ref("int_pessoa_fisica_all_cpf") }}),

    -- Fonte de dados de assistência social (cadastros)
    source_cadunico as (
        select b.cpf, b.dados[0] as dados  -- # TODO: corrigir na origem para não gerar um array de structs
        from all_cpf a
        inner join {{ source("rj-smas", "cadastros") }} b using (cpf_particao)
    ),

    -- Estrutura de CRAS por CPF
    cras_struct as (
        select
            cpf,
            dados.data_cadastro,
            dados.data_ultima_atualizacao,
            dados.data_limite_cadastro_atual_familia as data_limite_cadastro_atual,
            dados.estado_cadastral as status_cadastral,
        from source_cadunico
    ),

    dim_assistencia_social as (
        select
            all_cpf.cpf,
            struct(
                if(cras_struct.data_cadastro is not null, true, false) as indicador,
                struct(
                    cras_struct.data_cadastro,
                    cras_struct.data_ultima_atualizacao,
                    cras_struct.data_limite_cadastro_atual,
                    cras_struct.status_cadastral
                ) as cadunico
            ) as assistencia_social
        from all_cpf
        left join cras_struct using (cpf)
    )

select *
from dim_assistencia_social
