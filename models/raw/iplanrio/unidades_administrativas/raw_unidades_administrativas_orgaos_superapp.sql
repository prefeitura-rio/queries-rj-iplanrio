{{
    config(
        alias='orgaos_superapp'
    )
}}

SELECT
  cd_ua as cd_ua,
  sigla_ua as sigla_ua,
  nome_ua as nome_ua,
  cd_ua_pai as cd_ua_pai,
  nivel as nivel,
  ordem_ua_basica as ordem_ua_basica,
  ordem_absoluta as ordem_absoluta,
  ordem_relativa as ordem_relativa,
  msg as msg,
  updated_at as updated_at
    
FROM {{ source('unidades_administrativas_staging', 'orgaos') }}