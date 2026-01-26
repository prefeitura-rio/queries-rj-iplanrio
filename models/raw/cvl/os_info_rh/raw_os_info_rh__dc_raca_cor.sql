{{
    config(
        alias='dc_raca_cor',
        schema='os_info_rh'
    )
}}

SELECT
    `RACO_CD_RACA_COR`,
    `RACO_DS_DESCRICAO`
FROM {{ source('os_info_rh', 'dc_raca_cor') }}
