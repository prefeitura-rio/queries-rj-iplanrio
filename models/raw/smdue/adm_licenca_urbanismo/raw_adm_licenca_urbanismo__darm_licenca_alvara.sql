{{
  config(
    materialized='table',
    alias='darm_licenca_alvara'
  )
}}

SELECT
Num_Darm  AS id_darm ,
cod_Classe  AS id_classe_licenca ,
cod_Tipo_Lic  AS id_tipo_licenca ,
cod_Lic  AS cod_Lic,
cod_Compl_Lic  AS id_complemento_tipo_licenca ,
compl_Livre  AS comentario_licenca,
CAST(Valor AS FLOAT64)  AS valor ,
Formula  AS formula,

FROM {{ source('adm_licenca_urbanismo_staging', 'darm_licenca_alvara') }}
