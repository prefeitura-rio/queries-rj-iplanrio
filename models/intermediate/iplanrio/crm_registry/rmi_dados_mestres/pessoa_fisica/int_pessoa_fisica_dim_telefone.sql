-- Consolida contatos de pessoa física a partir de múltiplas fontes do município do
-- Rio de Janeiro
-- Este modelo gera um array de contatos por CPF, unificando dados de saúde e BCadastro
{{
    config(
        alias="dim_telefone",
        schema="intermediario_dados_mestres",
        materialized=("table" if target.name == "dev" else "ephemeral"),
    )
}}

with
    all_cpf as (select cpf, cpf_particao from {{ ref("int_pessoa_fisica_all_cpf") }}),

    -- CONTATO - TELEFONE
    -- Telefones consolidados e limpos da tabela intermediária
    telefone as (
        select
            origem_id as cpf,
            telefone_numero_completo, -- Manter o número completo para o join
            sistema_nome as origem,
            sistema_nome as sistema,
            data_atualizacao
        from {{ ref('int_telefones_raw_consolidated') }}
        where origem_tipo = 'CPF'
    ),

    telefone_enriquecido as (
        select
            t.*,
            tel.telefone_qualidade,
            tel.confianca_propriedade,
            tel.telefone_tipo,
            {{ classify_estrategia_envio('tel.telefone_qualidade', 'tel.confianca_propriedade') }} as estrategia_envio
        from telefone t
        left join {{ ref('int_telefones') }} tel on t.telefone_numero_completo = tel.telefone_numero_completo
    ),

    telefone_ranqueado as (
        select
            *,
            row_number() over (
                partition by cpf
                order by
                    case when telefone_tipo = 'CELULAR' then 1 else 2 end asc,
                    case
                        when confianca_propriedade = 'CONFIRMADA' then 1
                        when confianca_propriedade = 'MUITO_PROVAVEL' then 2
                        when confianca_propriedade = 'PROVAVEL' then 3
                        when confianca_propriedade = 'POUCO_PROVAVEL' then 4
                        when confianca_propriedade = 'IMPROVAVEL' then 5
                        else 6
                    end asc,
                    data_atualizacao desc
            ) as rank
        from telefone_enriquecido
    ),

    telefone_estruturado as (
        select
            cpf,
            array_agg(
                struct(
                    origem,
                    sistema,
                    {{ extract_ddi('telefone_numero_completo') }} as ddi,
                    {{ extract_ddd('telefone_numero_completo') }} as ddd,
                    {{ extract_numero('telefone_numero_completo') }} as valor,
                    telefone_qualidade as qualidade,
                    confianca_propriedade as confianca,
                    telefone_tipo as tipo,
                    estrategia_envio
                )
                order by rank asc
            ) as telefone
        from telefone_ranqueado
        group by cpf
    ),

    telefone_principal_alternativo as (
        select
            cpf,
            telefone[offset(0)] as principal,
            array(
                select as struct * except (pos)
                from unnest(telefone)
                with
                offset pos
                where pos > 0
            ) as alternativo
        from telefone_estruturado
    ),

    dim_telefone as (
        select
            a.cpf,
            struct(
                if(t.principal is not null, true, false) as indicador,
                t.principal,
                t.alternativo
            ) as telefone
        from all_cpf a
        left join telefone_principal_alternativo t using (cpf)
    )

select *
from dim_telefone
