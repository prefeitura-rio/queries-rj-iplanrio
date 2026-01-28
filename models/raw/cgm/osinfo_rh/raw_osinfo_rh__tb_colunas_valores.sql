{{
    config(
      alias="colunas_valores",
      description="Valores permitidos para colunas de outras tabelas, utilizados para validação ou configuração dinâmica.",
      materialized='table'
    )
}}

select
    safe_cast(`RHCO_COD_COLUNA` as string) as coluna_codigo,
    safe_cast(`RHCO_NM_COLUNA` as string) as coluna_nome,
    safe_cast(`RHGR_CD_CODIGO` as string) as grupo_codigo,
    safe_cast(`RHGR_TP_TIPO` as string) as grupo_tipo,
    safe_cast(`RHCO_PERC_PERCENTUAL` as float64) as coluna_percentual,
    safe_cast(`RHCO_NR_MES_VIGENCIA_INICIO` as decimal) as mes_vigencia_inicio,
    safe_cast(`RHCO_NR_MES_VIGENCIA_FIM` as decimal) as mes_vigencia_fim,
    safe_cast(`RHCO_NR_ANO_VIGENCIA_INICIO` as decimal) as ano_vigencia_inicio,
    safe_cast(`RHCO_NR_ANO_VIGENCIA_FIM` as decimal) as ano_vigencia_fim,
    safe_cast(`RHCO_NR_ORDEM` as decimal) as ordem,
    safe_cast(`RHCO_I18N_LABEL` as string) as i18n_label,
    safe_cast(`RHCO_CAMPO_OBRIGATORIO` as string) as campo_obrigatorio,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_colunas_valores') }}
