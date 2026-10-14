WITH
  extracao_chave AS (
    SELECT DISTINCT
      nome_arquivo,
      cnpj_modelo_normalizado AS cnpj_modelo,
      numero_nf_modelo_norm AS numero_nf_modelo
    FROM {{ ref("core_cgm_triagem_nf__extracao_pagina") }}
    WHERE numero_documento IS NOT NULL AND cnpj_emitente IS NOT NULL
  ),

  smf_nf_substituta_norm AS (
    SELECT
      *,
      LPAD(REGEXP_REPLACE(SAFE_CAST(cnpj_cpf_modelo AS STRING), r'[^0-9]', ''), 14, '0')
        AS cleaned_cnpj_modelo
    FROM `rj-nf-agent.poc_osinfo_ia.smf_nf_cancelada_substituta_20260311`
  ),

  smf_nf_verificacao_norm AS (
    SELECT
      cnpj,
      nfse,
      achei_nota_fiscal,
      data_cancelamento,
      LPAD(REGEXP_REPLACE(SAFE_CAST(cnpj AS STRING), r'[^0-9]', ''), 14, '0') AS cleaned_cnpj
    FROM `rj-nf-agent.brutos_smf.nf_verificacao_20260304`
    WHERE achei_nota_fiscal IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY cnpj, nfse ORDER BY data_cancelamento IS NOT NULL DESC) = 1
  )

SELECT
  e.nome_arquivo,
  e.cnpj_modelo,
  e.numero_nf_modelo,
  CASE
    WHEN v.achei_nota_fiscal IS NOT NULL AND v.data_cancelamento IS NULL
      THEN FALSE
    WHEN v.data_cancelamento IS NOT NULL
      THEN
        CASE
          WHEN
            sub.numero_nf_modelo IS NOT NULL
            AND IFNULL(sub.indicador_nf_substituta_declarada, 'TRUE') <> "FALSE"
            AND IFNULL(sub.indicador_nf_substituida, 'TRUE') <> "FALSE"
            THEN NULL
          ELSE TRUE
          END
    WHEN sub.numero_nf_modelo IS NULL THEN NULL
    WHEN sub.data_cancelamento IS NULL THEN FALSE
    WHEN
      (
        sub.indicador_nf_substituta_declarada = "FALSE"
        OR sub.indicador_nf_substituida = "FALSE")
      THEN TRUE
    ELSE NULL
    END AS cancelada,
  v.data_cancelamento AS cancelamento_data,
  sub.numero_nf_substituta AS cancelamento_substituta
FROM extracao_chave e
LEFT JOIN smf_nf_substituta_norm sub
  ON
    e.nome_arquivo = sub.nome_arquivo_declaracao
    AND e.numero_nf_modelo = sub.numero_nf_modelo
    AND e.cnpj_modelo = sub.cleaned_cnpj_modelo
LEFT JOIN smf_nf_verificacao_norm v
  ON
    e.cnpj_modelo = v.cleaned_cnpj
    AND e.numero_nf_modelo = v.nfse