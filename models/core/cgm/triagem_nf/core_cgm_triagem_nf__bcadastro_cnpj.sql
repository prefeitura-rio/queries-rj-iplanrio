WITH
  cnpjs_alvo AS (
    SELECT DISTINCT
      LPAD(REGEXP_REPLACE(cnpj_emitente, r'[^0-9]', ''), 14, '0') AS cnpj_norm
    FROM {{ ref("core_cgm_triagem_nf__extracao_pagina") }}
    WHERE cnpj_emitente IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT cnpj_cpf_normalizado
    FROM {{ ref("core_cgm_triagem_nf__osinfo_despesas") }}
    WHERE cnpj_cpf_normalizado IS NOT NULL
  ),

  dominio_atividade AS (
    SELECT
      LPAD(CAST(id AS STRING), 7, '0') AS id,
      descricao
    FROM `rj-iplanrio.brutos_bcadastro.dominio`
    WHERE column = 'atividade_economica' AND source = 'cnpj'
  )

SELECT
  c.cnpj,
  c.razao_social,
  c.uf,
  c.id_municipio,
  c.cnae_fiscal,
  d.descricao AS cnae_fiscal_descricao,
  c.inicio_atividade_data,
  c.situacao_cadastral,
  c.situacao_especial
FROM `rj-iplanrio.brutos_bcadastro.cnpj` c
INNER JOIN cnpjs_alvo t
  ON c.cnpj = t.cnpj_norm
LEFT JOIN dominio_atividade d
  ON c.cnae_fiscal = d.id