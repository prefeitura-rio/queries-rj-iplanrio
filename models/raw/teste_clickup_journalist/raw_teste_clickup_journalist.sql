-- tabela de CNO - Cadastro Nacional de Obras
{{
    config(
        materialized="table",
    )
}}


SELECT
  SAFE_CAST(
    REGEXP_REPLACE(cd_ua, r'\.0$', '') AS STRING
  ) as id_unidade_administrativa,
  CAST(
    REGEXP_REPLACE(sigla_ua, r'\.0$', '') AS INT64
  ) as sigla_unidade_administrativa,
  SAFE_CAST(
    REGEXP_REPLACE(nome_ua, r'\.0$', '') AS STRING
  ) as nome_unidade_administrativa,
  -- SAFE_CAST(
  --  REGEXP_REPLACE(cd_ua_pai, r'\.0$', '') AS STRING
  -- #) as id_unidade_administrativa_pai,
  -- COLUMN REMOVED FROM SCHEMA
  SAFE_CAST(
    REGEXP_REPLACE(nivel, r'\.0$', '') AS STRING
  ) as nivel,
  SAFE_CAST(
    REGEXP_REPLACE(ordem_ua_basica, r'\.0$', '') AS STRING
  ) as ordem_unidade_administrativa_basica,
  SAFE_CAST(
    REGEXP_REPLACE(ordem_absoluta, r'\.0$', '') AS STRING
  ) as ordem_absoluta,
  SAFE_CAST(
    REGEXP_REPLACE(ordem_relativa, r'\.0$', '') AS STRING
  ) as ordem_relativa,
  CAST(
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S%Ez', updated_at) 
    AS DATETIME
  ) AS updated_at
    
FROM `rj-iplanrio.unidades_administrativas_staging.orgaos`