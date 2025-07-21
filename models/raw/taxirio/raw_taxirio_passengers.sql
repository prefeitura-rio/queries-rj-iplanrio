{{
    config(   
        schema="brutos_taxirio",
        alias="passageiros",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day"
        },  
        tags=["raw", "taxirio"],
        description="Tabela de Passageiros"
    )
}}

SELECT
  
  safe_cast(id as string) as id_passageiro,
  safe_cast(user as string) as id_usuario,
  datetime(timestamp(createdAt)) as data_criacao,
  datetime(timestamp(createdAt)) as data_criacao_particao,
  safe_cast(login as string) as usuario,
  safe_cast(password as string) as senha,
  safe_cast(salt as string) as aleatorio,
  safe_cast(isAbleToUsePaymentInApp as bool) as pode_usar_pagamento_app,
  datetime(timestamp(infoPhone_updatedAt)) as data_atualizacao_telefone,
  safe_cast(infoPhone_id as string) as id_telefone,
  safe_cast(infoPhone_appVersion as string) as versao_app_telefone,
  safe_cast(infoPhone_phoneModel as string) as modelo_telefone,
  safe_cast(infoPhone_phoneManufacturer as string) as fabricante_telefone,
  safe_cast(infoPhone_osVersion as string) as versao_sistema_telefone,
  safe_cast(infoPhone_osName as string) as nome_sistema_telefone,
  SAFE_CAST (tokenInfo_httpSalt as STRING) as ficha_http_aleatorio,
  SAFE_CAST (tokenInfo_wssSalt as STRING) as ficha_wss_aleatorio,
  SAFE_CAST (tokenInfo_pushToken as STRING) as ficha_envio,
 
FROM
  {{ source('brutos_taxirio_staging','passengers') }}
