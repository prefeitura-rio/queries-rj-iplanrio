-- Consolida contatos de pessoa física a partir de múltiplas fontes do município do
-- Rio de Janeiro
-- Este modelo gera um array de contatos de email por CPF, unificando dados de saúde e
-- BCadastro
{{
    config(
        alias="dim_email",
        schema="intermediario_dados_mestres",
        materialized=("table" if target.name == "dev" else "ephemeral"),
    )
}}

with
    all_cpf as (select cpf, cpf_particao from {{ ref("int_pessoa_fisica_all_cpf") }}),

    source_bcadastro as (
        select b.*
        from all_cpf a
        inner join {{ source("brutos_bcadastro", "cpf") }} b using (cpf_particao)
    ),

    source_sms as (
        select b.*
        from all_cpf a
        inner join {{ source("rj-sms", "paciente") }} b using (cpf_particao)
    ),

    -- CONTATO - EMAIL
    email as (
        select
            cpf, lower(email.valor) as valor, email.rank, 'sms' as origem, email.sistema
        from source_sms, unnest(contato.email) as email
        union all
        select
            cpf,
            lower(contato.email) as valor,
            1 as rank,
            'receita_federal' as origem,
            'bcadastro' as sistema
        from source_bcadastro
        where contato.email is not null
    ),

    email_ranqueado as (
        select
            *,
            case
                when origem = 'receita_federal' then 1 when origem = 'sms' then 2 else 3
            end as rank_origem
        from email
    ),

    email_estruturado as (
        select
            cpf,
            array_agg(
                struct(origem, sistema, valor) order by rank_origem asc, rank asc
            ) as email
        from email_ranqueado
        group by cpf
    ),

    email_principal_alternativo as (
        select
            cpf,
            email[offset(0)] as principal,
            array(
                select as struct * except (pos)
                from unnest(email)
                with
                offset pos
                where pos > 0
            ) as alternativo
        from email_estruturado
    ),

    dim_email as (
        select
            cpf,
            struct(
                if(principal is not null, true, false) as indicador,
                principal,
                alternativo
            ) as email
        from all_cpf
        left join email_principal_alternativo using (cpf)
    )

select *
from dim_email
