{{
    config(
        alias="equipamentos_saude_territorio_equipe_familia",
        schema="plus_codes",
        materialized="table",
    )
}}

WITH
  estabelecimento_esfera AS (
    SELECT
      id_cnes,
      esfera
    FROM
      {{ source("saude_dados_mestres", "estabelecimento") }}
  )

SELECT
  -- Plus Codes (não podem ser gerados sem coordenadas de ponto)
  CAST(NULL AS STRING) AS plus11,
  
  -- Identificação principal
  id_equipe AS id_equipamento,
  'SMS' AS secretaria_responsavel,
  TRIM(t.categoria) AS tipo_equipamento,
  t.nome_area AS nome_oficial,
  CONCAT('MEDICOS:\n', TRIM(t.medicos), '\n\nENFERMEIROS:\n', TRIM(t.enfermeiros)) AS nome_popular,
  
  -- Mais Plus Codes (nulos)
  CAST(NULL AS STRING) AS plus10,
  CAST(NULL AS STRING) AS plus8,
  CAST(NULL AS STRING) AS plus6,
  
  -- Detalhes de localização (coordenadas de ponto não disponíveis nesta etapa)
  CAST(NULL AS FLOAT64) AS latitude,
  CAST(NULL AS FLOAT64) AS longitude,
  t.geometry, -- Mantém a coluna de polígono original

  -- Endereço estruturado (informações não disponíveis na origem)
  STRUCT(
    CAST(NULL AS STRING) AS logradouro,
    CAST(NULL AS STRING) AS numero,
    CAST(NULL AS STRING) AS complemento,
    CAST(NULL AS STRING) AS bairro,
    CAST(NULL AS STRING) AS cep
  ) AS endereco,
  
  -- Bairro estruturado (informações parciais da origem)
  STRUCT(
    CAST(NULL AS STRING) AS id_bairro,
    CAST(NULL AS STRING) AS bairro,
    t.area_planejamento AS regiao_planejamento,
    CAST(NULL AS STRING) AS regiao_administrativa,
    CAST(NULL AS STRING) AS subprefeitura
  ) AS bairro,
  
  -- Informações de contato
  STRUCT(
    SPLIT(REPLACE(t.telefone, ' ', ''), '/') AS telefones,
    CAST(NULL AS STRING) AS email,
    CAST(NULL AS STRING) AS site,
    STRUCT(
      CAST(NULL AS STRING) AS facebook,
      CAST(NULL AS STRING) AS instagram,
      CAST(NULL AS STRING) AS twitter
    ) AS redes_social
  ) AS contato,
  
  -- Flags de status (assumindo que todas as equipes estão ativas)
  TRUE AS ativo,
  FALSE AS aberto_ao_publico,
  
  -- Horário de funcionamento (informação não disponível na origem)
  CAST([] AS ARRAY<STRUCT<dia STRING, abre TIME, fecha TIME>>) AS horario_funcionamento,
  
  -- Fonte dos dados
  '{{ ref("raw_equipamentos_saude_equipes_arcgis") }}' AS fonte,
  
  -- Vigência (informação não disponível na origem)
  CAST(NULL AS DATE) AS vigencia_inicio,
  CAST(NULL AS DATE) AS vigencia_fim,
  
  -- Esfera (city, state, or federal level) from estabelecimento join
  e.esfera AS esfera,
  
  -- Metadados (colunas da origem não utilizadas diretamente)
  TO_JSON_STRING(
    STRUCT(
      t.area_planejamento,
      t.nome_area,
      t.id_ine,
      t.tipo_equipe,
      t.medicos,
      t.enfermeiros
    )
  ) AS metadata,
  
  -- Timestamp da atualização
  CURRENT_TIMESTAMP() AS updated_at
FROM
  {{ ref("raw_equipamentos_saude_equipes_arcgis") }} AS t
LEFT JOIN
  estabelecimento_esfera AS e
ON
  t.cnes = e.id_cnes