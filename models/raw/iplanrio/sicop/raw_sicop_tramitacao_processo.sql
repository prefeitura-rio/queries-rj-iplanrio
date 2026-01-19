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
  safe_cast(mat_logado as int64)                            as matricula_logado,
  safe_cast(mat_recebedor as int64)                         as matricula_recebedor,
  
  -- Código de despacho
  safe_cast(cod_despacho as string)                         as id_despacho,

  -- Datas da tramitação
  --safe_cast(data_despacho as date)                          as data_despacho,
  safe_cast(CONCAT( SUBSTR(data_despacho,7,4),'-', SUBSTR(data_despacho,4,2) ,'-', SUBSTR(data_despacho,1,2) ) as date)         as data_despacho,
  --safe_cast(data_saida as date)                             as data_saida,
  safe_cast(CONCAT( SUBSTR(data_saida,7,4),'-', SUBSTR(data_saida,4,2) ,'-', SUBSTR(data_saida,1,2) ) as date)         as data_saida,
  
  -- Órgãos envolvidos na tramitação
  safe_cast(org_origem as int64)                            as orgao_origem,
  safe_cast(org_destino as int64)                           as orgao_destino,
  safe_cast(org_transcritor as int64)                       as orgao_transcritor,

  safe_cast(SUBSTR(_prefect_extracted_at,1,10) as date)      as datalake_transformed_at 

from {{ source("brutos_sicop_staging","tramitacao_processo") }}

