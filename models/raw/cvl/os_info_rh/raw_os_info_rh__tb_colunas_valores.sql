{{
    config(
      alias="tb_colunas_valores",
      description="Valores permitidos para colunas de outras tabelas, utilizados para validação ou configuração dinâmica."
    )
}}

select
    safe_cast(`RHCO_COD_COLUNA` as integer) as coluna_codigo,
    safe_cast(`RHCO_NM_COLUNA` as string) as coluna_nome,
    safe_cast(`RHGR_CD_CODIGO` as integer) as grupo_codigo,
    safe_cast(`RHGR_TP_TIPO` as string) as grupo_tipo,
    safe_cast(`RHCO_PERC_PERCENTUAL` as float64) as coluna_percentual,
    safe_cast(`RHCO_NR_MES_VIGENCIA_INICIO` as integer) as mes_vigencia_inicio,
    safe_cast(`RHCO_NR_MES_VIGENCIA_FIM` as integer) as mes_vigencia_fim,
    safe_cast(`RHCO_NR_ANO_VIGENCIA_INICIO` as integer) as ano_vigencia_inicio,
    safe_cast(`RHCO_NR_ANO_VIGENCIA_FIM` as integer) as ano_vigencia_fim,
    safe_cast(`RHCO_NR_ORDEM` as integer) as ordem,
    safe_cast(`RHCO_I18N_LABEL` as string) as i18n_label,
    safe_cast(`RHCO_CAMPO_OBRIGATORIO` as string) as campo_obrigatorio,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'tb_colunas_valores') }}
