{{
  config(
    schema= 'brutos_taxirio',
    alias= 'usuarios',
    materialized='table',
    partition_by={
      'field': 'data_particao',
      'data_type': 'date'
    },


)}}
SELECT
  SAFE_CAST (id as STRING) as id_usuario,
  SAFE_CAST (displayName as STRING) as nome_exibicao,
  SAFE_CAST (fullName as STRING) as nome_completo,
  SAFE_CAST (email as STRING) as email,
  SAFE_CAST (phoneNumber as STRING) as telefone,
  SAFE_CAST (cpf as INT64) as cpf,
  SAFE.PARSE_DATE('%d/%m/%Y', createdAt) AS data_criacao,
  SAFE.PARSE_DATE('%d/%m/%Y',birthDate) as data_nascimento,
  SAFE_CAST (validadoReceita as BOOL) as receita_validada,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  SAFE_CAST (federalRevenueData_name as STRING) as nome_receita_federal,
  SAFE.PARSE_DATE('%d/%m/%Y',federalRevenueData_birthDate) as data_nascimento_receita_federal,
  SAFE_CAST (federalRevenueData_mothersName as STRING) as nome_mae_receita_federal,     
  SAFE_CAST (federalRevenueData_yearOfDeath as STRING) as ano_morte_receita_federal,          
  SAFE_CAST (federalRevenueData_sex as STRING) as sexo_receita_federal,          
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  DATE(SAFE_CAST(ano_particao AS INT64), SAFE_CAST(mes_particao AS INT64), 1) AS data_particao

  
FROM
  `rj-iplanrio.brutos_taxirio_staging.users`
