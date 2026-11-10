SELECT
  id_documento,
  resultado_extracao_ia,
  pagina_nf_extracao_ia,
  cnpj_cpf_extracao_ia,
  valor_documento_extracao_ia
FROM {{ ref('core_cgm_triagem_nf__extracao_ia_match') }}
WHERE
  (
    resultado_extracao_ia = 'NF Encontrada' 
    AND (
      pagina_nf_extracao_ia IS NULL
      OR cnpj_cpf_extracao_ia IS NULL
      OR ids_grupo_nf IS NULL
    )
  )
  OR
  (
    resultado_extracao_ia IN ('NF Sem Match', 'NF Não Encontrada') 
    AND (
      pagina_nf_extracao_ia IS NOT NULL
      OR cnpj_cpf_extracao_ia IS NOT NULL
      OR valor_documento_extracao_ia IS NOT NULL
    )
  )
