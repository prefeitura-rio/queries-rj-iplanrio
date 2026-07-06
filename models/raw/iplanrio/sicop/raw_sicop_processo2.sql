{{
    config(
        alias="processo2",
        description="Dados brutos de processo2 do SICOP (VW_PROCESSO2_DLK)"
    )
}}


-- Conversões e padronização de nomes seguindo o padrão de raw_sicop_processo
select
  safe_cast(num_processo as string)                          as id_processo,
  safe_cast(num_processo_principal as string)               as num_processo_principal,
  safe_cast(CONCAT( SUBSTR(data_processo,7,4),'-', SUBSTR(data_processo,4,2) ,'-', SUBSTR(data_processo,1,2) ) as date) as data_processo,
  safe_cast(CONCAT( SUBSTR(data_sistema,7,4),'-', SUBSTR(data_sistema,4,2) ,'-', SUBSTR(data_sistema,1,2) ) as date) as data_sistema,

  case
    when REGEXP_CONTAINS(hora_processo, r'^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$')
      then PARSE_TIME("%H:%M:%S", hora_processo)
    when REGEXP_CONTAINS(hora_processo, r'^[0-2][0-9]:[0-5][0-9]$')
      then PARSE_TIME("%H:%M", hora_processo)
    else null
  end as hora_entra,

  -- Documento
  safe_cast(documento as string)                            as cpf_cgc,
  safe_cast(orgao_documento as string)                      as org_doc,
  safe_cast(tipo_documento as string)                        as tipo_docto,

  -- Assunto
  safe_cast(codigo_assunto as string)                       as cod_assu_p,
  safe_cast(descricao_assunto as string)                    as desc_assun,

  -- Requerente / contato
  safe_cast(requerente as string)                           as requerente,

  -- Status
  safe_cast(situacao_processo as string)                    as status,
  safe_cast(sta_dig_registro as string)                    as status_dig,

  -- Quantidades / volumes
  safe_cast(qtd_volumes as int64)                          as qtd_vol,
  safe_cast(num_volume as int64)                           as volume,

  -- Órgãos / transcrições
  safe_cast(org_origem as string)                           as org_origem,
  safe_cast(org_transcritor as string)                      as org_transc,
  safe_cast(org_alteracao as string)                        as org_alt,
  safe_cast(org_destino as string)                          as org_dest,

  safe_cast(codigo_logradouro as string)                    as cod_logr,

  -- Metadados de extração
  safe_cast(SUBSTR(_prefect_extracted_at,1,10) as date)     as datalake_transformed_at,
  CAST(_prefect_extracted_at AS TIMESTAMP)                  as loaded_at
from {{ source('brutos_sicop_staging', 'processo2') }}
