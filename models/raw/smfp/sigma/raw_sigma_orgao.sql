{{
    config(
        alias='orgao',
        description="Cadastro de Órgãos do SIGMA"
    )
}}

SELECT
  SAFE_CAST(cd_orgao AS NUMERIC) AS codigo_orgao,
  SAFE_CAST(tp_orgao AS NUMERIC) AS tipo_orgao,
  SAFE_CAST(ds_tipo_orgao AS STRING) AS descricao_tipo_orgao,
  SAFE_CAST(cd_orgao_pai AS NUMERIC) AS codigo_orgao_pai,
  SAFE_CAST(cd_secretaria_sdi AS NUMERIC) AS cd_secretaria_sdi,
  SAFE_CAST(descricao AS STRING) AS descricao_orgao,
  SAFE_CAST(endereco AS STRING) AS endereco_orgao,
  SAFE_CAST(complemento AS STRING) AS complemento_endereco,
  SAFE_CAST(cep AS NUMERIC) AS cep,
  SAFE_CAST(numero_porta AS NUMERIC) AS numero_porta,
  SAFE_CAST(fax1 AS NUMERIC) AS fax_1,
  SAFE_CAST(fax2 AS NUMERIC) AS fax_2,
  SAFE_CAST(tel1 AS NUMERIC) AS telefone_1,
  SAFE_CAST(tel2 AS NUMERIC) AS telefone_2,
  SAFE_CAST(sigla AS STRING) AS sigla_orgao,
  SAFE_CAST(email AS STRING) AS email_orgao,
  SAFE_CAST(tp_unidade AS STRING) AS tipo_unidade,
  SAFE_CAST(st_status AS STRING) AS status_orgao,
  SAFE_CAST(cnes AS NUMERIC) AS codigo_saude,
  SAFE_CAST(matricula_responsavel AS NUMERIC) AS matricula_responsavel,
  SAFE_CAST(nm_responsavel AS STRING) AS nome_responsavel,
  SAFE_CAST(dt_responsavel AS NUMERIC) AS data_cadastramento
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'orgao') }}