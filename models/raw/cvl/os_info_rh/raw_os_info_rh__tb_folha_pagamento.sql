{{
    config(
        alias='tb_folha_pagamento',
        schema='os_info_rh'
    )
}}

SELECT
    `FPTO_CD_FOLHA`,
    `VINC_CD_VINCULO`,
    `FPTO_NR_MES_REFERENCIA`,
    `FPTO_NR_ANO_REFERENCIA`,
    `FPTO_NR_MES_COMPETENCIA`,
    `FPTO_NR_ANO_COMPETENCIA`,
    `ID_CONTRATO`,
    `COD_UNIDADE`,
    `FPTO_VL_CARGA_HORARIA_EFETIVA`,
    `FPTO_PERC_RATEIO`,
    `FPTO_DT_LICENCA_INICIO`,
    `FPTO_DT_LICENCA_FIM`,
    `FPTO_OBSERVACAO`
FROM {{ source('os_info_rh', 'tb_folha_pagamento') }}
