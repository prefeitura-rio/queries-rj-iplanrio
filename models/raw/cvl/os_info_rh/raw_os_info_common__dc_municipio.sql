{{
    config(
        alias='dc_municipio',
        schema='os_info_common'
    )
}}

SELECT
    `MUNI_CD_IBGE`,
    `UF_CD_IBGE`,
    `MUNI_DS_NOME`
FROM {{ source('os_info_common', 'dc_municipio') }}
