{{
    config(
        alias='dc_grau_instrucao',
        schema='os_info_rh'
    )
}}

SELECT
    `GINS_CD_GRAU_INSTRUCAO`,
    `GINS_DS_GRAU_INSTRUCAO`,
    `GINS_ORDEM`
FROM {{ source('os_info_rh', 'dc_grau_instrucao') }}
