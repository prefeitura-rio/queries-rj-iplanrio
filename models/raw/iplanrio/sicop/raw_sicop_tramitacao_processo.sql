{{
    config(
        alias="tramitacao_processo",
        description="Dados brutos de tramitação de processos do SICOP (VW_TRAMITACAO_PROCESSO_DLK)"
    )
}}


-- Conversões e padronização de nomes conforme guia de estilo
select
  -- Chave primária do processo
  safe_cast(num_processo as string)                         as id_processo,
  
  -- Histórico de tramitações  
  safe_cast(seq as int64)                                   as sequencial,
  
  -- Identificadores de guia e remessa
  safe_cast(num_guia as int64)                              as numero_guia,
  
  -- Matrículas relacionadas à tramitação
  safe_cast(mat_despacho as int64)                          as matricula_despacho,
  safe_cast(mat_dig_tramitacao as int64)                    as matricula_digitacao_tramitacao,
  safe_cast(mat_dig_recebimento as int64)                   as matricula_digitacao_recebimento,
  
  -- Código de despacho
  safe_cast(cod_despacho as string)                         as id_despacho,

  -- Datas da tramitação
  safe_cast(CONCAT( SUBSTR(data_despacho,7,4),'-', SUBSTR(data_despacho,4,2) ,'-', SUBSTR(data_despacho,1,2) ) as date)         as data_despacho,
  --safe_cast(data_saida as date)                             as data_saida,
  safe_cast(CONCAT( SUBSTR(data_saida,7,4),'-', SUBSTR(data_saida,4,2) ,'-', SUBSTR(data_saida,1,2) ) as date)         as data_saida,
  safe_cast(CONCAT( SUBSTR(dt_dig_tramitacao,7,4),'-', SUBSTR(dt_dig_tramitacao,4,2) ,'-', SUBSTR(dt_dig_tramitacao,1,2) ) as date)  as data_digitacao_tramitacao,
  safe_cast(CONCAT( SUBSTR(dt_dig_recebimento,7,4),'-', SUBSTR(dt_dig_recebimento,4,2) ,'-', SUBSTR(dt_dig_recebimento,1,2) ) as date) as data_digitacao_recebimento,
  
  -- Órgãos envolvidos na tramitação
  safe_cast(org_origem as int64)                            as orgao_origem,
  safe_cast(org_destino as int64)                           as orgao_destino,
  safe_cast(org_transcritor as int64)                       as orgao_transcritor,

  safe_cast(SUBSTR(_prefect_extracted_at,1,10) as date)      as datalake_transformed_at 

from {{ source("brutos_sicop_staging","tramitacao_processo") }}

