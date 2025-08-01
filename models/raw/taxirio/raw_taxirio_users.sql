{{
    config(
        alias="usuarios",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day",
        },
        description="Tabela de Usuarios",
    )
}}

select
    safe_cast(id as string) as id_usuario,
    safe_cast(displayname as string) as nome_usuario,
    safe_cast(fullname as string) as nome_completo_usuario,
    safe_cast(email as string) as email_usuario,
    safe_cast(phonenumber as string) as telefone_usuario,
    safe_cast(cpf as string) as cpf_usuario,
    safe.parse_datetime('%d/%m/%Y', createdat) as data_criacao,
    safe.parse_datetime('%d/%m/%Y', createdat) as data_criacao_particao,
    safe.parse_date('%d/%m/%Y', birthdate) as data_nascimento,
    safe_cast(validadoreceita as bool) as receita_validada,
    safe_cast(federalrevenuedata_name as string) as nome_receita_federal,
    safe.parse_date(
        '%d/%m/%Y', federalrevenuedata_birthdate
    ) as data_nascimento_receita_federal,
    safe_cast(federalrevenuedata_mothersname as string) as nome_mae_receita_federal,
    safe_cast(federalrevenuedata_yearofdeath as string) as ano_morte_receita_federal,
    safe_cast(federalrevenuedata_sex as string) as sexo_receita_federal

from {{ source("brutos_taxirio_staging", "users") }}
