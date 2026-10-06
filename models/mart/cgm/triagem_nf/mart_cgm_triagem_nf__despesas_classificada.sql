WITH
  ep AS (
    SELECT
      nome_arquivo,
      MAX(data_geracao) AS data_processamento,
      MAX(timestamp_geracao) AS timestamp_processamento
    FROM {{ ref("core_cgm_triagem_nf__extracao_pagina") }}
    GROUP BY nome_arquivo
  ),

  od AS (
    SELECT d.*
    FROM {{ ref("core_cgm_triagem_nf__osinfo_despesas") }} d
    WHERE d.nome_arquivo IN (SELECT nome_arquivo FROM ep)
  ),

  epd AS (
    SELECT e.*
    FROM {{ ref("core_cgm_triagem_nf__extracao_ia_match") }} e
    WHERE e.id_documento IN (SELECT id_documento FROM od)
  ),

  bc as (
    SELECT * FROM {{ ref("core_cgm_triagem_nf__bcadastro_cnpj") }}
  ),

  canc as (
    select * from {{ ref("core_cgm_triagem_nf__nf_cancelamento") }}
  ),

  -- =========================================================================
  -- APONTAMENTOS - REGRAS DE NEGÓCIO
  -- =========================================================================
  apontamentos_base AS (
    SELECT
      od.id_documento,

      (
        od.cod_organizacao <> od.cod_unidade
        AND epd.valor_pago_total_documento > (epd.valor_documento_extracao_ia + 1)
        AND epd.ids_grupo_nf LIKE '%,%') AS apontamento_duplicidade_indicador,
      CASE
        WHEN
          od.cod_organizacao <> od.cod_unidade
          AND epd.valor_pago_total_documento > (epd.valor_documento_extracao_ia + 1)
          AND epd.ids_grupo_nf LIKE '%,%'
          THEN epd.ids_grupo_nf
        ELSE NULL
        END AS apontamento_duplicidade_ids,

      canc.cancelada AS apontamento_cancelamento_indicador,

      -- Mantido NULL: bcadastro_cnpj traz situacao_cadastral ATUAL (hoje),
      -- não o status na data de emissão da NF
      CAST(NULL AS BOOL) AS apontamento_cnpj_inativo_indicador,

      (epd.valor_pago_total_documento > (epd.valor_documento_extracao_ia + 1))
        AS apontamento_valor_pago_excedente_indicador,

      CASE
        WHEN epd.data_emissao_extracao_ia IS NULL THEN NULL
        WHEN bc.inicio_atividade_data IS NULL THEN NULL
        WHEN DATE(bc.inicio_atividade_data) > DATE(epd.data_emissao_extracao_ia)
          THEN TRUE
        ELSE FALSE
        END AS apontamento_emissao_anterior_abertura_indicador,

      (od.data_emissao <> epd.data_emissao_extracao_ia)
        AS apontamento_emissao_divergente_indicador,

      (od.valor_documento <> epd.valor_documento_extracao_ia)
        AS apontamento_valor_divergente_indicador
    FROM od
    LEFT JOIN epd ON epd.id_documento = od.id_documento
    LEFT JOIN canc
      ON
        canc.nome_arquivo = od.nome_arquivo
        AND canc.cnpj_modelo = epd.cnpj_cpf_extracao_ia
        AND canc.numero_nf_modelo = epd.numero_documento_extracao_ia_norm
    LEFT JOIN bc
      ON epd.cnpj_cpf_extracao_ia = bc.cnpj
  ),

  apontamentos AS (
    SELECT
      *,
      CASE
        WHEN
          apontamento_duplicidade_indicador IS TRUE
          OR apontamento_cancelamento_indicador IS TRUE
          OR apontamento_cnpj_inativo_indicador IS TRUE
          OR apontamento_valor_pago_excedente_indicador IS TRUE
          OR apontamento_emissao_anterior_abertura_indicador IS TRUE
          THEN 'Grave'
        WHEN
          apontamento_emissao_divergente_indicador IS TRUE
          OR apontamento_valor_divergente_indicador IS TRUE
          THEN 'Leve'
        ELSE 'Descartado'
        END AS apontamento_classificacao_indicadores
    FROM apontamentos_base
  )

-- =========================================================================
-- CLASSIFICAÇÃO FINAL
-- =========================================================================
SELECT
  od.id_documento,
  od.numero_contrato AS contrato_numero,
  od.periodo_vigencia_contrato AS contrato_periodo_vigencia,
  od.secretaria_nome,
  od.cod_organizacao AS id_organizacao,
  od.organizacao_nome,
  od.cod_unidade AS id_unidade,
  od.unidade_nome,
  od.data_envio AS declaracao_envio_data,
  od.nome_arquivo AS declaracao_arquivo_nome,
  od.referencia_ano AS declaracao_referencia_ano,
  od.referencia_mes AS declaracao_referencia_mes,
  od.numero_documento AS declaracao_numero_documento,
  od.cnpj_cpf_normalizado AS declaracao_cnpj_cpf,
  od.valor_documento AS declaracao_documento_valor,
  od.valor_pago AS declaracao_pago_valor,
  od.data_emissao AS declaracao_emissao_data,
  od.data_pagamento AS declaracao_pagamento_data,
  epd.resultado_extracao_ia AS extracao_resultado,
  epd.pagina_nf_extracao_ia AS extracao_pagina,
  epd.tipo_documento_extracao_ia AS extracao_documento_tipo,
  epd.numero_documento_extracao_ia AS extracao_numero_documento,
  epd.cnpj_cpf_extracao_ia AS extracao_cnpj_cpf,
  epd.valor_documento_extracao_ia AS extracao_documento_valor,
  epd.data_emissao_extracao_ia AS extracao_emissao_data,
  epd.arquivo_paginas_com_match AS extracao_paginas_com_match,
  epd.arquivo_paginas_sem_match AS extracao_paginas_sem_match,
  bc.razao_social AS fornecedor_razao_social,
  bc.uf AS fornecedor_uf,
  bc.cnae_fiscal_descricao AS fornecedor_cnae_descricao,
  bc.situacao_cadastral AS fornecedor_situacao_cadastral_atual,
  CASE
    WHEN epd.resultado_extracao_ia = "NF Sem Match" THEN "Não avaliado"
    WHEN epd.resultado_extracao_ia = "NF Não Encontrada" THEN "Grave"
    WHEN epd.resultado_extracao_ia = "NF Encontrada"
      THEN ap.apontamento_classificacao_indicadores
    ELSE NULL
    END AS apontamento_resultado,
  ap.apontamento_duplicidade_indicador,
  ap.apontamento_duplicidade_ids,
  ap.apontamento_cancelamento_indicador,
  ap.apontamento_cnpj_inativo_indicador,
  ap.apontamento_valor_pago_excedente_indicador,
  ap.apontamento_emissao_anterior_abertura_indicador,
  ap.apontamento_emissao_divergente_indicador,
  ap.apontamento_valor_divergente_indicador,
  ep.data_processamento,
  ep.timestamp_processamento
  ep.versao_pipeline
FROM od
LEFT JOIN ep
  ON od.nome_arquivo = ep.nome_arquivo
LEFT JOIN epd USING (id_documento)
LEFT JOIN apontamentos ap USING (id_documento)
LEFT JOIN bc
  ON epd.cnpj_cpf_extracao_ia = bc.cnpj