{{
    config(
        alias='dc_conselho',
        schema='os_info_rh'
    )
}}

SELECT
    `CONS_SG_CONSELHO`,
    `CONS_DS_CONSELHO`
FROM {{ source('os_info_rh', 'dc_conselho') }}
