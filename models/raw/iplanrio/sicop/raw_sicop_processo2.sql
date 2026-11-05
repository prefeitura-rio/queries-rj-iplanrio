{{
    config(
        alias="processo2",
        description="Dados brutos de processo2 do SICOP (VW_PROCESSO2_DLK)"
    )
}}


-- Conversões e padronização de nomes seguindo o padrão de raw_sicop_processo
SELECT
  safe_cast (num_processo AS string) AS id_processo,
  safe_cast (num_processo_principal AS string) AS num_processo_principal,
  safe_cast (
    CONCAT(
      SUBSTR(data_processo, 7, 4),
      '-',
      SUBSTR(data_processo, 4, 2),
      '-',
      SUBSTR(data_processo, 1, 2)
    ) AS date
  ) AS data_processo,
  safe_cast (
    CONCAT(
      SUBSTR(data_sistema, 7, 4),
      '-',
      SUBSTR(data_sistema, 4, 2),
      '-',
      SUBSTR(data_sistema, 1, 2)
    ) AS date
  ) AS data_sistema,
  CASE
    WHEN REGEXP_CONTAINS (
      hora_processo,
      r'^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'
    ) THEN PARSE_TIME ("%H:%M:%S", hora_processo)
    WHEN REGEXP_CONTAINS (hora_processo, r'^[0-2][0-9]:[0-5][0-9]$') THEN PARSE_TIME ("%H:%M", hora_processo)
    ELSE NULL
  END AS hora_entra,
  -- Documento
  safe_cast (documento AS string) AS cpf_cgc,
  safe_cast (orgao_documento AS string) AS org_doc,
  safe_cast (tipo_documento AS string) AS tipo_docto,
  -- Assunto
  safe_cast (codigo_assunto AS string) AS cod_assu_p,
  safe_cast (descricao_assunto AS string) AS desc_assun,
  -- Requerente / contato
  safe_cast (requerente AS string) AS requerente,
  -- Status
  safe_cast (situacao_processo AS string) AS status,
  safe_cast (sta_dig_registro AS string) AS status_dig,
  -- Quantidades / volumes
  safe_cast (qtd_volumes AS int64) AS qtd_vol,
  safe_cast (num_volume AS int64) AS volume,
  -- Órgãos / transcrições
  safe_cast (org_origem AS string) AS org_origem,
  safe_cast (org_transcritor AS string) AS org_transc,
  safe_cast (org_alteracao AS string) AS org_alt,
  safe_cast (org_destino AS string) AS org_dest,
  safe_cast (codigo_logradouro AS string) AS cod_logr,
  -- Metadados de extração
  safe_cast (_prefect_extracted_at AS date) AS datalake_transformed_at,
from {{ source('brutos_sicop_staging', 'processo2') }}
