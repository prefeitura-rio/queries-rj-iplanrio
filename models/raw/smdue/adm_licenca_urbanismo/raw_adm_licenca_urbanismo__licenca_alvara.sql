{{
  config(
    materialized='table',
    alias='licenca_alvara'
  )
}}

SELECT
id_lic AS id_licenca,
num_lic AS id_licenciamento,
id_edif AS id_edificacao,
cod_classe AS id_classe_licenca,
cod_tipo_lic AS id_tipo_licenca,
cod_lic AS cod_lic,
cod_compl_lic AS id_complemento_tipo_licenca,
compl_livre AS comentario_licenca,
CAST(dec9218 AS BOOL) AS decreto_9218,
outrodec AS outro_decreto,
CAST(obrasconcluidas AS BOOL) AS obras_concluidas,

FROM {{ source('adm_licenca_urbanismo_staging', 'licenca_alvara') }}
