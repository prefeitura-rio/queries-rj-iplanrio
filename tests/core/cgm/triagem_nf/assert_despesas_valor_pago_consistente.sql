{{ config(severity = 'warn') }}

-- Falha se o valor pago declarado em uma linha de despesa for maior do que o valor total informado para o documento fiscal.
SELECT
  id_documento,
  nome_arquivo,
  numero_documento,
  valor_documento,
  valor_pago,
  (valor_pago - valor_documento) AS excedente_declarado
FROM {{ ref('core_cgm_triagem_nf__osinfo_despesas') }}
WHERE
  valor_pago IS NOT NULL
  AND valor_documento IS NOT NULL
  AND valor_pago > (valor_documento + 1)