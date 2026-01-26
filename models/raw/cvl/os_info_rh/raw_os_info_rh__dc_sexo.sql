{{
    config(
        alias='dc_sexo',
        schema='os_info_rh'
    )
}}

SELECT
    `SEX_CD_SEXO`,
    `SEX_DS_DESCRICAO`
FROM {{ source('os_info_rh', 'dc_sexo') }}
