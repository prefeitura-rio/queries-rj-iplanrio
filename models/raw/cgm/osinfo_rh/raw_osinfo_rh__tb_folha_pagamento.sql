{{
    config(
      alias="folha_pagamento",
      description="Folhas de pagamento consolidadas de funcionários para um determinado período de competência.",
      materialized='table'
    )
}}

select
    safe_cast(tfp.`FPTO_CD_FOLHA` as int64) as folha_codigo,
    safe_cast(tfp.`VINC_CD_VINCULO` as int64) as vinculo_codigo,
    safe_cast(tfp.`FPTO_NR_MES_REFERENCIA` as int64) as mes_referencia,
    safe_cast(tfp.`FPTO_NR_ANO_REFERENCIA` as int64) as ano_referencia,
    safe_cast(tfp.`FPTO_NR_MES_COMPETENCIA` as int64) as mes_competencia,
    safe_cast(tfp.`FPTO_NR_ANO_COMPETENCIA` as int64) as ano_competencia,
    safe_cast(tfp.`ID_CONTRATO` as int64) as contrato_id,
    safe_cast(tfp.`COD_UNIDADE` as int64) as unidade_codigo,
    safe_cast(tfp.`FPTO_VL_CARGA_HORARIA_EFETIVA` as numeric) as carga_horaria_efetiva,
    safe_cast(tfp.`FPTO_PERC_RATEIO` as numeric) as percentual_rateio,
    safe_cast(tfp.`FPTO_DT_LICENCA_INICIO` as date) as licenca_inicio_data,
    safe_cast(tfp.`FPTO_DT_LICENCA_FIM` as date) as licenca_fim_data,
    safe_cast(tfp.`FPTO_OBSERVACAO` as string) as observacao,
    safe_cast(SUM(CASE WHEN tcv.rhgr_cd_codigo = 'P' THEN safe_cast(tvfp.vlfp_vl_valor AS NUMERIC) ELSE 0 END)
  + SUM(CASE WHEN tcv.rhgr_cd_codigo = 'R' THEN safe_cast(tvfp.vlfp_vl_valor AS NUMERIC) ELSE 0 END) as numeric)
    AS rem_bruta,

    safe_cast(SUM(CASE WHEN tcv.rhgr_cd_codigo = 'D' THEN safe_cast(tvfp.vlfp_vl_valor AS NUMERIC) ELSE 0 END) as numeric)
    AS descontos,
    safe_cast((
    SUM(CASE WHEN tcv.rhgr_cd_codigo IN ('P','R') THEN safe_cast(tvfp.vlfp_vl_valor AS NUMERIC) ELSE 0 END) - SUM(CASE WHEN tcv.rhgr_cd_codigo = 'D' THEN safe_cast(tvfp.vlfp_vl_valor AS NUMERIC) ELSE 0 END)
    ) as numeric) AS rem_liquida,
    safe_cast(SUBSTR(tfp.`_prefect_extracted_at`,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_folha_pagamento') }} tfp
left join {{ source('brutos_osinfo_rh_staging','tb_valores_folha_pagamento') }} tvfp on tfp.fpto_cd_folha = tvfp.fpto_cd_folha
left join {{ source('brutos_osinfo_rh_staging','tb_colunas_valores') }} tcv on tvfp.rhco_cod_coluna = tcv.rhco_cod_coluna
group by tfp.FPTO_CD_FOLHA, tfp.VINC_CD_VINCULO, tfp.FPTO_NR_MES_REFERENCIA, tfp.FPTO_NR_ANO_REFERENCIA, tfp.FPTO_NR_MES_COMPETENCIA, tfp.FPTO_NR_ANO_COMPETENCIA, tfp.ID_CONTRATO, tfp.COD_UNIDADE, tfp.FPTO_VL_CARGA_HORARIA_EFETIVA, tfp.FPTO_PERC_RATEIO, tfp.FPTO_DT_LICENCA_INICIO, tfp.FPTO_DT_LICENCA_FIM, tfp.FPTO_OBSERVACAO, tfp._prefect_extracted_at