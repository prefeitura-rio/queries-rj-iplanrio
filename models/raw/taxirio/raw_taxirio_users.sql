{{
    config(
        schema="brutos_taxirio",
        alias="usuarios",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day"
        },
        tags=["raw", "taxirio"],
        description="Tabela de Usuarios"
    )
}}  

SELECT
  safe_cast(id as string) as id_usuario,
  safe_cast(displayName as string) as nome_usuario,
  safe_cast(fullName as string) as nome_completo_usuario,
  safe_cast(email as string) as email_usuario,
  safe_cast(phoneNumber as string) as telefone_usuario,
  safe_cast(cpf as string) as cpf_usuario,
  safe.parse_datetime('%d/%m/%Y', createdAt) as data_criacao,
  safe.parse_datetime('%d/%m/%Y', createdAt) as data_criacao_particao,
  safe.parse_date('%d/%m/%Y',birthDate) as data_nascimento,
  safe_cast(validadoReceita as bool) as receita_validada,
  safe_cast(federalRevenueData_name as string) as nome_receita_federal,
  safe.parse_date('%d/%m/%Y',federalRevenueData_birthDate) as data_nascimento_receita_federal,
  safe_cast(federalRevenueData_mothersName as string) as nome_mae_receita_federal,
  safe_cast(federalRevenueData_yearOfDeath as string) as ano_morte_receita_federal,
  safe_cast(federalRevenueData_sex as string) as sexo_receita_federal
   
FROM
  {{  source('brutos_taxirio_staging','users')}}
