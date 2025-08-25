-- Consolida informações de ocupação de pessoa física a partir de múltiplas fontes do
-- município do Rio de Janeiro
-- Este modelo gera um struct de informações de ocupação por CPF unificando dados do ergon

{{
    config(
        alias="dim_ocupacao",
        schema="intermediario_dados_mestres",
        materialized=("table" if target.name == "dev" else "ephemeral"),
        tags=["daily"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}
with
    -- Fonte de CPFs
    all_cpf as (select cpf, cpf_particao from {{ ref("int_pessoa_fisica_all_cpf") }}),
    -- todo adicionar cpfs do ergon no all_cpf

    source_ergon as (
        select b.cpf, b.trabalha_prefeitura, b.cpf_particao  -- # TODO: corrigir na origem para não gerar um array de structs
        from all_cpf a
        inner join {{ ref("int_pessoa_fisica_dim_ergon") }} b using (cpf_particao)
    ),

    -- FINAL TABLE
    final as (
        select
            -- Primary Key
            a.cpf,
            struct(
                source_ergon.trabalha_prefeitura
            ) AS ocupacao,
            cast(a.cpf as int64) as cpf_particao
        FROM all_cpf a
        LEFT JOIN source_ergon USING(cpf_particao)
    )

    SELECT * FROM final