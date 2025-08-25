{{
    config(
        alias='orgao',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(cd_orgao AS INT64) AS codigo_orgao,
  SAFE_CAST(tp_orgao AS INT64) AS tipo_orgao,
  SAFE_CAST(ds_tipo_orgao AS STRING) AS descricao_tipo_orgao,
  SAFE_CAST(cd_orgao_pai AS INT64) AS codigo_orgao_pai,
  SAFE_CAST(cd_secretaria_sdi AS INT64) AS cd_secretaria_sdi,
  SAFE_CAST(descricao AS STRING) AS descricao_orgao,
  SAFE_CAST(endereco AS STRING) AS endereco_orgao,
  SAFE_CAST(complemento AS STRING) AS complemento_endereco,
  SAFE_CAST(cep AS INT64) AS cep,
  SAFE_CAST(numero_porta AS INT64) AS numero_porta,
  SAFE_CAST(fax1 AS INT64) AS fax_1,
  SAFE_CAST(fax2 AS INT64) AS fax_2,
  SAFE_CAST(tel1 AS INT64) AS telefone_1,
  SAFE_CAST(tel2 AS INT64) AS telefone_2,
  SAFE_CAST(sigla AS STRING) AS sigla_orgao,
  SAFE_CAST(email AS STRING) AS email_orgao,
  SAFE_CAST(tp_unidade AS STRING) AS tipo_unidade,
  SAFE_CAST(st_status AS STRING) AS status_orgao,
  SAFE_CAST(cnes AS INT64) AS codigo_saude,
  SAFE_CAST(matricula_responsavel AS INT64) AS matricula_responsavel,
  SAFE_CAST(nm_responsavel AS STRING) AS nome_responsavel,
  SAFE_CAST(dt_responsavel AS STRING) AS data_cadastramento
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_ORGAO')}}