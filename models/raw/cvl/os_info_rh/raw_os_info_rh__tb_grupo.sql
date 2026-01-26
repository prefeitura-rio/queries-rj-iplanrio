{{
    config(
        alias='tb_grupo',
        schema='os_info_rh'
    )
}}

SELECT
    `RHGR_TP_TIPO`,
    `RHGR_CD_CODIGO`,
    `RHGR_NM_NOME`,
    `RHGR_IN_TOTALIZAR`
FROM {{ source('os_info_rh', 'tb_grupo') }}
