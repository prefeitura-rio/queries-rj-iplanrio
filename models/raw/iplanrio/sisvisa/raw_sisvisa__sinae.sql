{{config (alias='sinae', materialized="table")}}

SELECT 
CAST(CODIGO AS string) as codigo,
CAST(DESCRICAO AS string) as descricao,
CAST(INSCRICAOMUNICIPAL as string) as inscricao_municipal,
CAST(INSCRICAOMUNICIPAL_ORIG as string) as inscricao_municipal_orig,
CAST(_airbyte_extracted_at as DATETIME) as loaded_at
FROM {{source('brutos_sisvisa_staging', 'Vw_Rest_Alvara_Estab_Sinae_Datalake')}}