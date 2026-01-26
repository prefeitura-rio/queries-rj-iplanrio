{{
    config(
      alias="tb_provisionamento",
      description="Provisionamentos de verbas trabalhistas de funcionários para um período, garantindo a reserva contábil."
    )
}}

select
    safe_cast(`PROV_CD_PROVISIONAMENTO` as integer) as provisionamento_codigo,
    safe_cast(`VINC_CD_VINCULO` as integer) as vinculo_codigo,
    safe_cast(`PROV_NR_MES_REFERENCIA` as integer) as mes_referencia,
    safe_cast(`PROV_NR_ANO_REFERENCIA` as integer) as ano_referencia,
    safe_cast(`PROV_NR_MES_COMPETENCIA` as integer) as mes_competencia,
    safe_cast(`PROV_NR_ANO_COMPETENCIA` as integer) as ano_competencia,
    safe_cast(`ID_CONTRATO` as integer) as contrato_id,
    safe_cast(`COD_UNIDADE` as string) as unidade_codigo,
    safe_cast(`PROV_OBSERVACAO` as string) as observacao,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'tb_provisionamento') }}
