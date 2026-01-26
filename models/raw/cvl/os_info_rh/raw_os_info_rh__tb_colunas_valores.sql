{{
    config(
        alias='tb_colunas_valores',
        schema='os_info_rh'
    )
}}

SELECT
    `RHCO_COD_COLUNA`,
    `RHCO_NM_COLUNA`,
    `RHGR_CD_CODIGO`,
    `RHGR_TP_TIPO`,
    `RHCO_PERC_PERCENTUAL`,
    `RHCO_NR_MES_VIGENCIA_INICIO`,
    `RHCO_NR_MES_VIGENCIA_FIM`,
    `RHCO_NR_ANO_VIGENCIA_INICIO`,
    `RHCO_NR_ANO_VIGENCIA_FIM`,
    `RHCO_NR_ORDEM`,
    `RHCO_I18N_LABEL`,
    `RHCO_CAMPO_OBRIGATORIO`
FROM {{ source('os_info_rh', 'tb_colunas_valores') }}
