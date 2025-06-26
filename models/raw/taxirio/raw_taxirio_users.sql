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
  safe_cast(isActive as bool) as usuario_ativo,
  safe_cast(isAdmin as bool) as usuario_admin,
  safe_cast(isDriver as bool) as usuario_motorista,
  safe_cast(isPassenger as bool) as usuario_passageiro,
  safe_cast(isBlocked as bool) as usuario_bloqueado,
  safe_cast(isDeleted as bool) as usuario_excluido,
  safe_cast(createdBy as string) as criado_por,
  safe_cast(updatedBy as string) as atualizado_por,
  safe_cast(driverId as string) as id_motorista,
  safe_cast(passengerId as string) as id_passageiro,
  safe_cast(city as string) as cidade_usuario,
  safe_cast(state as string) as estado_usuario,
  safe_cast(country as string) as pais_usuario,
  safe_cast(lastLogin as datetime) as ultimo_login,
  safe_cast(lastLogout as datetime) as ultimo_logout,
  safe_cast(lastPasswordChange as datetime) as ultima_troca_senha,
  safe_cast(lastEmailChange as datetime) as ultima_troca_email,
  safe_cast(lastPhoneChange as datetime) as ultima_troca_telefone,
  safe_cast(lastAdminChange as datetime) as ultima_troca_admin,
  safe_cast(lastDriverChange as datetime) as ultima_troca_motorista,
  safe_cast(lastPassengerChange as datetime) as ultima_troca_passageiro,
  safe_cast(lastBlockedChange as datetime) as ultima_troca_bloqueio,
  safe_cast(lastDeletedChange as datetime) as ultima_troca_exclusao,
  safe_cast(lastCityChange as datetime) as ultima_troca_cidade,
  safe_cast(lastStateChange as datetime) as ultima_troca_estado,
  safe_cast(lastCountryChange as datetime) as ultima_troca_pais,
  safe_cast(lastLoginIp as string) as ultimo_ip_login,
  safe_cast(lastLogoutIp as string) as ultimo_ip_logout,
  safe_cast(lastPasswordChangeIp as string) as ultimo_ip_troca_senha,
  safe_cast(lastEmailChangeIp as string) as ultimo_ip_troca_email,
  safe_cast(lastPhoneChangeIp as string) as ultimo_ip_troca_telefone,
  safe_cast(lastAdminChangeIp as string) as ultimo_ip_troca_admin,
  safe_cast(lastDriverChangeIp as string) as ultimo_ip_troca_motorista,
  safe_cast(lastPassengerChangeIp as string) as ultimo_ip_troca_passageiro,
  safe_cast(lastBlockedChangeIp as string) as ultimo_ip_troca_bloqueio,
  safe_cast(lastDeletedChangeIp as string) as ultimo_ip_troca_exclusao,
  safe_cast(lastCityChangeIp as string) as ultimo_ip_troca_cidade,
  safe_cast(lastStateChangeIp as string) as ultimo_ip_troca_estado,
  safe_cast(lastCountryChangeIp as string) as ultimo_ip_troca_pais,
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
