{{
  config(alias="extracao_ia_match")
}}

WITH
  despesas_base AS (
    SELECT
      od.id_documento,
      od.nome_arquivo,
      od.numero_documento_norm,
      od.cnpj_cpf_normalizado,
      {{ tratar_numero_documento("od.numero_documento") }} AS candidatos_numero
    FROM {{ ref("core_cgm_triagem_nf__osinfo_despesas") }} od
  ),

  paginas_base AS (
    SELECT
      p.nome_arquivo,
      p.pagina,
      p.numero_documento,
      p.numero_nf_modelo_norm,
      p.cnpj_modelo_normalizado,
      p.cnpj_emitente,
      p.valor_documento,
      p.data_emissao_documento,
      p.tipo_documento_extracao,
      {{ tratar_numero_documento("p.numero_documento") }} AS candidatos_numero
    FROM {{ ref("core_cgm_triagem_nf__extracao_pagina") }} p
  ),

  -- Pares despesa x página com mesmo CNPJ e número igual (exato) ou com
  -- interseção entre os radicais tratados do número (macro tratar_numero_documento)
  pares_match_bruto AS (
    SELECT
      d.id_documento,
      p.nome_arquivo,
      p.pagina,
      COALESCE(d.numero_documento_norm = p.numero_nf_modelo_norm, FALSE) AS match_exato
    FROM despesas_base d
    INNER JOIN paginas_base p
      ON p.nome_arquivo = d.nome_arquivo
     AND p.cnpj_modelo_normalizado = d.cnpj_cpf_normalizado
    WHERE
      COALESCE(d.numero_documento_norm = p.numero_nf_modelo_norm, FALSE)
      OR EXISTS (
        SELECT 1
        FROM UNNEST(d.candidatos_numero) AS num_d
        INNER JOIN UNNEST(p.candidatos_numero) AS num_p
          ON num_d = num_p
      )
  ),

  -- O match tratado só vale para despesas sem nenhum match exato no arquivo
  pares_match AS (
    SELECT id_documento, nome_arquivo, pagina
    FROM pares_match_bruto
    QUALIFY match_exato OR NOT LOGICAL_OR(match_exato) OVER (PARTITION BY id_documento)
  ),

  match_por_declaracao_valor AS (
    SELECT
      od.id_documento,
      LOGICAL_OR(pm.id_documento IS NOT NULL) AS tem_match
    FROM {{ ref("core_cgm_triagem_nf__osinfo_despesas") }} od
    INNER JOIN {{ ref("core_cgm_triagem_nf__extracao_pagina") }} p
      ON p.nome_arquivo = od.nome_arquivo
    LEFT JOIN pares_match pm
      ON pm.id_documento = od.id_documento
     AND pm.pagina = p.pagina
    GROUP BY od.id_documento
  ),

  pares_agrupado AS (
    SELECT
      nome_arquivo,
      pagina,
      ARRAY_AGG(id_documento ORDER BY id_documento) AS id_documentos
    FROM pares_match
    GROUP BY nome_arquivo, pagina
  ),

  paginas_com_match_valor AS (
    SELECT
      p.nome_arquivo,
      p.pagina,
      p.numero_documento,
      p.numero_nf_modelo_norm,
      p.cnpj_modelo_normalizado,
      p.valor_documento AS valor_total_documento,
      p.data_emissao_documento,
      p.tipo_documento_extracao AS tipo_documento,
      COALESCE(pa.id_documentos, []) AS id_documentos_correspondentes
    FROM paginas_base p
    LEFT JOIN pares_agrupado pa
      ON pa.nome_arquivo = p.nome_arquivo
     AND pa.pagina = p.pagina
    WHERE p.numero_documento IS NOT NULL AND p.cnpj_emitente IS NOT NULL
  ),

  nf_extraida_sem_match_valor AS (
    SELECT
      nome_arquivo,
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(id_documentos_correspondentes) = 0,
          STRUCT(pagina, numero_documento AS numero_nf_modelo, cnpj_modelo_normalizado AS cnpj_modelo,
                 valor_total_documento, data_emissao_documento),
          NULL)
          IGNORE NULLS
        ORDER BY pagina) AS paginas_sem_match
    FROM paginas_com_match_valor
    GROUP BY nome_arquivo
  ),

  paginas_com_match_valor_por_arquivo AS (
    SELECT
      nome_arquivo,
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(id_documentos_correspondentes) > 0,
          STRUCT(pagina, numero_documento AS numero_nf_modelo, cnpj_modelo_normalizado AS cnpj_modelo,
                 valor_total_documento, data_emissao_documento, id_documentos_correspondentes),
          NULL)
          IGNORE NULLS
        ORDER BY pagina) AS paginas_com_match
    FROM paginas_com_match_valor
    GROUP BY nome_arquivo
  ),

  extracao_por_documento AS (
    SELECT
      id_documento,
      pagina AS pagina_nf_extracao_ia,
      numero_documento AS numero_documento_extracao_ia,
      numero_nf_modelo_norm AS numero_documento_extracao_ia_norm,
      cnpj_modelo_normalizado AS cnpj_cpf_extracao_ia,
      valor_total_documento AS valor_documento_extracao_ia,
      tipo_documento AS tipo_documento_extracao_ia,
      data_emissao_documento AS data_emissao_extracao_ia
    FROM paginas_com_match_valor, UNNEST(id_documentos_correspondentes) AS id_documento
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_documento ORDER BY pagina) = 1
  ),

  metricas_por_documento AS (
    SELECT
      od.id_documento,
      SUM(od.valor_pago)
        OVER (PARTITION BY epd.cnpj_cpf_extracao_ia, epd.numero_documento_extracao_ia)
        AS valor_pago_total_documento,
      DENSE_RANK()
        OVER (
          PARTITION BY epd.cnpj_cpf_extracao_ia, epd.numero_documento_extracao_ia
          ORDER BY od.data_envio, od.id_documento
        ) AS rank_declaracao,
      STRING_AGG(CAST(od.id_documento AS STRING), ', ')
        OVER (PARTITION BY epd.cnpj_cpf_extracao_ia, epd.numero_documento_extracao_ia)
        AS ids_grupo_nf
    FROM {{ ref("core_cgm_triagem_nf__osinfo_despesas") }} od
    INNER JOIN extracao_por_documento epd
      ON od.id_documento = epd.id_documento
  )

SELECT
  od.id_documento,
  CASE
    WHEN mval.tem_match THEN "NF Encontrada"
    WHEN ARRAY_LENGTH(COALESCE(s.paginas_sem_match, [])) > 0 THEN "NF Sem Match"
    ELSE "NF Não Encontrada"
    END AS resultado_extracao_ia,
  epd.pagina_nf_extracao_ia,
  epd.tipo_documento_extracao_ia,
  epd.numero_documento_extracao_ia,
  epd.numero_documento_extracao_ia_norm,
  epd.cnpj_cpf_extracao_ia,
  epd.valor_documento_extracao_ia,
  epd.data_emissao_extracao_ia,
  CASE
    WHEN ARRAY_LENGTH(COALESCE(c.paginas_com_match, [])) > 0 THEN TO_JSON_STRING(c.paginas_com_match)
    ELSE NULL
    END AS arquivo_paginas_com_match,
  CASE
    WHEN ARRAY_LENGTH(COALESCE(s.paginas_sem_match, [])) > 0 THEN TO_JSON_STRING(s.paginas_sem_match)
    ELSE NULL
    END AS arquivo_paginas_sem_match,
  m.valor_pago_total_documento,
  m.rank_declaracao,
  m.ids_grupo_nf
FROM {{ ref("core_cgm_triagem_nf__osinfo_despesas") }} od
INNER JOIN match_por_declaracao_valor mval ON mval.id_documento = od.id_documento
LEFT JOIN nf_extraida_sem_match_valor s ON s.nome_arquivo = od.nome_arquivo
LEFT JOIN paginas_com_match_valor_por_arquivo c ON c.nome_arquivo = od.nome_arquivo
LEFT JOIN extracao_por_documento epd ON epd.id_documento = od.id_documento
LEFT JOIN metricas_por_documento m ON m.id_documento = od.id_documento