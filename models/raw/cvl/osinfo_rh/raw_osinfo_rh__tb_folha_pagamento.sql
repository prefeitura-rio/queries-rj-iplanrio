{{
    config(
      alias="tb_folha_pagamento",
      description="Folhas de pagamento consolidadas de funcionários para um determinado período de competência."
    )
}}

select
    safe_cast(`FPTO_CD_FOLHA` as integer) as folha_codigo,
    safe_cast(`VINC_CD_VINCULO` as integer) as vinculo_codigo,
    safe_cast(`FPTO_NR_MES_REFERENCIA` as integer) as mes_referencia,
    safe_cast(`FPTO_NR_ANO_REFERENCIA` as integer) as ano_referencia,
    safe_cast(`FPTO_NR_MES_COMPETENCIA` as integer) as mes_competencia,
    safe_cast(`FPTO_NR_ANO_COMPETENCIA` as integer) as ano_competencia,
    safe_cast(`ID_CONTRATO` as integer) as contrato_id,
    safe_cast(`COD_UNIDADE` as integer) as unidade_codigo,
    safe_cast(`FPTO_VL_CARGA_HORARIA_EFETIVA` as float64) as carga_horaria_efetiva,
    safe_cast(`FPTO_PERC_RATEIO` as float64) as percentual_rateio,
    safe_cast(`FPTO_DT_LICENCA_INICIO` as date) as licenca_inicio_data,
    safe_cast(`FPTO_DT_LICENCA_FIM` as date) as licenca_fim_data,
    safe_cast(`FPTO_OBSERVACAO` as string) as observacao,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_folha_pagamento') }}
