{{
    config(
        alias='dc_unidade_federacao',
        schema='os_info_common'
    )
}}

SELECT
    `UF_CD_IBGE`,
    `UF_SG_SIGLA`,
    `UF_DS_DESCRICAO`
FROM {{ source('os_info_common', 'dc_unidade_federacao') }}
