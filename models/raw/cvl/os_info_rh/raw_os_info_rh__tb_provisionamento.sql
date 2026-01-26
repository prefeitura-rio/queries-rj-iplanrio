{{
    config(
        alias='tb_provisionamento',
        schema='os_info_rh'
    )
}}

SELECT
    `PROV_CD_PROVISIONAMENTO`,
    `VINC_CD_VINCULO`,
    `PROV_NR_MES_REFERENCIA`,
    `PROV_NR_ANO_REFERENCIA`,
    `PROV_NR_MES_COMPETENCIA`,
    `PROV_NR_ANO_COMPETENCIA`,
    `ID_CONTRATO`,
    `COD_UNIDADE`,
    `PROV_OBSERVACAO`
FROM {{ source('os_info_rh', 'tb_provisionamento') }}
