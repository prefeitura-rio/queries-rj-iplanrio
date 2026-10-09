WITH
  despesas_norm AS (
    SELECT
      * EXCEPT (num_documento),
      REGEXP_REPLACE(descricao, r'(?i)\.pdf$', '') AS nome_arquivo,
      LPAD(REGEXP_REPLACE(COALESCE(cpf, cnpj), r'[^0-9]', ''), 14, '0')
        AS cnpj_cpf_normalizado,
      num_documento AS numero_documento,
      SAFE_CAST(REGEXP_REPLACE(CAST(num_documento AS STRING), r'[^0-9]', '') AS INT64)
        AS numero_documento_norm
    FROM `rj-cvl.adm_contrato_gestao.despesas`
    WHERE
      id_tipo_documento = "1"
      AND data_envio BETWEEN DATE("2021-11-01") AND DATE("2026-08-31")
  ),

  contrato_secretaria AS (
    SELECT
      c.id_contrato,
      c.numero_contrato,
      s.id_secretaria,
      s.secretaria,
      CASE
        WHEN SAFE.PARSE_DATE('%d/%m/%Y', c.periodo_vigencia) IS NOT NULL
          THEN SAFE.PARSE_DATE('%d/%m/%Y', c.periodo_vigencia)
        WHEN SAFE.PARSE_DATE('%d-%m-%Y', c.periodo_vigencia) IS NOT NULL
          THEN SAFE.PARSE_DATE('%d-%m-%Y', c.periodo_vigencia)
        WHEN SAFE.PARSE_DATE('%d-%m/%Y', c.periodo_vigencia) IS NOT NULL
          THEN SAFE.PARSE_DATE('%d-%m/%Y', c.periodo_vigencia)
        WHEN
          SAFE.PARSE_DATE(
            '%d/%m/%Y',
            REPLACE(
              REGEXP_EXTRACT(c.periodo_vigencia, r'(?i)a\s*([0-9]{2}[-/][0-9]{2}[-/][0-9]{4})'),
              '-', '/'))
          IS NOT NULL
          THEN
            SAFE.PARSE_DATE(
              '%d/%m/%Y',
              REPLACE(
                REGEXP_EXTRACT(c.periodo_vigencia, r'(?i)a\s*([0-9]{2}[-/][0-9]{2}[-/][0-9]{4})'),
                '-', '/'))
        ELSE NULL
        END AS periodo_vigencia_contrato
    FROM `rj-cvl.adm_contrato_gestao.secretaria` s
    LEFT JOIN `rj-cvl.adm_contrato_gestao.contrato` c
      USING (id_secretaria)
  ),

  administracao_unidade AS (
    SELECT cod_unidade, nome_fantasia
    FROM `rj-cvl.adm_contrato_gestao.administracao_unidade`
  )

SELECT
  d.*,
  cs.numero_contrato,
  cs.periodo_vigencia_contrato,
  cs.secretaria AS secretaria_nome,
  org.nome_fantasia AS organizacao_nome,
  uni.nome_fantasia AS unidade_nome
FROM despesas_norm d
LEFT JOIN contrato_secretaria cs
  USING (id_contrato)
LEFT JOIN administracao_unidade uni
  ON uni.cod_unidade = d.cod_unidade
LEFT JOIN administracao_unidade org
  ON org.cod_unidade = d.cod_organizacao