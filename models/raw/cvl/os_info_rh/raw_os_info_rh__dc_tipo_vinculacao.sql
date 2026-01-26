{{
    config(
        alias='dc_tipo_vinculacao',
        schema='os_info_rh'
    )
}}

SELECT
    `TPVC_CD_TIPO_VINCULACAO`,
    `TPVC_DS_DESCRICAO`
FROM {{ source('os_info_rh', 'dc_tipo_vinculacao') }}
