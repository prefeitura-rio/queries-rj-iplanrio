{{
    config(
        alias='tb_valores_folha_pagamento',
        schema='os_info_rh'
    )
}}

SELECT
    `FPTO_CD_FOLHA`,
    `RHCO_COD_COLUNA`,
    `VLFP_VL_VALOR`
FROM {{ source('os_info_rh', 'tb_valores_folha_pagamento') }}
