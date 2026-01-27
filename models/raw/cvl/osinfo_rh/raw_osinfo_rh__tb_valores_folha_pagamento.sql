{{
    config(
      alias="tb_valores_folha_pagamento",
      description="Valores de eventos (rubricas) detalhados na folha de pagamento de um funcionário para um período."
    )
}}

select
    safe_cast(`FPTO_CD_FOLHA` as integer) as folha_codigo,
    safe_cast(`RHCO_COD_COLUNA` as integer) as coluna_codigo,
    safe_cast(`VLFP_VL_VALOR` as float64) as valor,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_valores_folha_pagamento') }}
