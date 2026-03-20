

SELECT
  {{ clean_and_cast('cd_ua', 'string') }} as id_unidade_administrativa,
  {{ clean_and_cast('sigla_ua', 'string') }} as sigla_unidade_administrativa,
  {{ clean_and_cast('nome_ua', 'string') }} as nome_unidade_administrativa,
  -- SAFE_CAST(
  --  REGEXP_REPLACE(cd_ua_pai, r'\.0$', '') AS STRING
  -- #) as id_unidade_administrativa_pai,
  -- COLUMN REMOVED FROM SCHEMA
  {{ clean_and_cast('nivel', 'string') }} as nivel,
  {{ clean_and_cast('ordem_ua_basica', 'string') }} as ordem_unidade_administrativa_basica,
  {{ clean_and_cast('ordem_absoluta', 'string') }} as ordem_absoluta,
  {{ clean_and_cast('ordem_relativa', 'string') }} as ordem_relativa,
  CAST(
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S%Ez', updated_at) 
    AS DATETIME
  ) AS updated_at
    
FROM `rj-iplanrio.unidades_administrativas_staging.orgaos`