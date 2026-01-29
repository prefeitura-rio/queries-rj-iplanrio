{{
    config(
      alias="valores_folha_pagamento",
      description="Valores de eventos (rubricas) detalhados na folha de pagamento de um funcionário para um período.",  
    )
}}

select
    safe_cast(`FPTO_CD_FOLHA` as int64) as folha_codigo,
    safe_cast(`RHCO_COD_COLUNA` as string) as coluna_codigo,
    safe_cast(`VLFP_VL_VALOR` as numeric) as valor,
    DATETIME(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    DATETIME(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_valores_folha_pagamento') }}
