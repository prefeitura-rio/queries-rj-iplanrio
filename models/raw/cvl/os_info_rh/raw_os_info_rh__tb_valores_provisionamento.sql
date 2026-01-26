{{
    config(
        alias='tb_valores_provisionamento',
        schema='os_info_rh'
    )
}}

SELECT
    `PROV_CD_PROVISIONAMENTO`,
    `RHCO_COD_COLUNA`,
    `VLPR_VL_VALOR`
FROM {{ source('os_info_rh', 'tb_valores_provisionamento') }}
