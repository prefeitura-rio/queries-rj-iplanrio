{{
    config(
      alias="folha_pagamento",
      description="Folhas de pagamento consolidadas de funcionários para um determinado período de competência.",
    )
}}

select
    safe_cast(`FPTO_CD_FOLHA` as int64) as folha_codigo,
    safe_cast(`VINC_CD_VINCULO` as int64) as vinculo_codigo,
    safe_cast(`FPTO_NR_MES_REFERENCIA` as int64) as mes_referencia,
    safe_cast(`FPTO_NR_ANO_REFERENCIA` as int64) as ano_referencia,
    safe_cast(`FPTO_NR_MES_COMPETENCIA` as int64) as mes_competencia,
    safe_cast(`FPTO_NR_ANO_COMPETENCIA` as int64) as ano_competencia,
    safe_cast(`ID_CONTRATO` as int64) as contrato_id,
    safe_cast(`COD_UNIDADE` as int64) as unidade_codigo,
    safe_cast(`FPTO_VL_CARGA_HORARIA_EFETIVA` as numeric) as carga_horaria_efetiva,
    safe_cast(`FPTO_PERC_RATEIO` as numeric) as percentual_rateio,
    safe_cast(`FPTO_DT_LICENCA_INICIO` as date) as licenca_inicio_data,
    safe_cast(`FPTO_DT_LICENCA_FIM` as date) as licenca_fim_data,
    safe_cast(`FPTO_OBSERVACAO` as string) as observacao,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_folha_pagamento') }}
