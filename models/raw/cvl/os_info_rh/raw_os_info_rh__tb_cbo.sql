{{
    config(
        alias='tb_cbo',
        schema='os_info_rh'
    )
}}

SELECT
    `CBO_CD_CBO`,
    `CBO_DS_CBO`,
    `CBO_IN_ATIVIDADE_FIM`
FROM {{ source('os_info_rh', 'tb_cbo') }}
