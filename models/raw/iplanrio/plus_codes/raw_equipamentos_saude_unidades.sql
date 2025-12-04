{{
    config(
        alias="equipamentos_saude_territorio_unidades",
        schema="plus_codes",
        materialized="table",
    )
}}

WITH
  saude_unidades_processed AS (
    SELECT
      cnes,
      area_planejamento,
      nome_unidade,
      categoria,
      logradouro,
      numero,
      complemento,
      cep,
      bairro,
      telefone,
      email,
      email2,
      site,
      CAST(hora_abre AS INT64) AS hora_abre,
      CAST(hora_fecha AS INT64) AS hora_fecha,
      CAST(hora_abre_sabado AS INT64) AS hora_abre_sabado,
      CAST(hora_fecha_sabado AS INT64) AS hora_fecha_sabado,
      CAST(funciona_24_horas AS INT64) AS funciona_24_horas,
      facebook,
      instagram,
      twitter,
      latitude_unidade,
      longitude_unidade,
      geometry
    FROM
      {{ ref("raw_equipamentos_saude_unidades_arcgis") }}
  ),
  estabelecimento_esfera AS (
    SELECT
      id_cnes,
      esfera
    FROM
      {{ source("saude_dados_mestres", "estabelecimento") }}
  ),
  tb AS (
    SELECT
      -- Pluscodes
      coalesce(tools.encode_pluscode(t.latitude_unidade, t.longitude_unidade, 11), '') as plus11,
      coalesce(tools.encode_pluscode(t.latitude_unidade, t.longitude_unidade, 10), '') as plus10,
      coalesce(tools.encode_pluscode(t.latitude_unidade, t.longitude_unidade, 8), '') as plus8,
      coalesce(tools.encode_pluscode(t.latitude_unidade, t.longitude_unidade, 6), '') as plus6,
      -- Core identification
      t.cnes AS id_equipamento,
      'SMS' AS secretaria_responsavel,
      t.categoria AS tipo_equipamento,
      TRIM(REGEXP_REPLACE(t.nome_unidade, r'SMS ', '')) AS nome_oficial,
      TRIM(REGEXP_REPLACE(REGEXP_REPLACE(t.nome_unidade, r'SMS ', ''), r' - AP \d{2}$', '')) AS nome_popular,
      -- Location details
      t.latitude_unidade  AS latitude,
      t.longitude_unidade AS longitude,
      t.geometry,
      -- Structured address
      STRUCT(
        UPPER(t.logradouro) AS logradouro,
        t.numero AS numero,
        UPPER(t.complemento) AS complemento,
        UPPER(t.bairro) AS bairro,
        t.cep AS cep
      ) AS endereco,
      -- Structured bairro information from join
      STRUCT(
        b.id_bairro AS id_bairro,
        UPPER(b.nome) AS bairro,
        UPPER(b.nome_regiao_planejamento) AS regiao_planejamento,
        UPPER(b.nome_regiao_administrativa) AS regiao_administrativa,
        UPPER(b.subprefeitura) AS subprefeitura
      ) AS bairro,
      -- Structured contact information
      STRUCT(
        ARRAY(
          SELECT
            phone
          FROM
            UNNEST([t.telefone]) AS phone
          WHERE
            phone IS NOT NULL AND TRIM(phone) != ''
        ) AS telefones,
        t.email AS email,
        t.site AS site,
        STRUCT(
          t.facebook AS facebook,
          t.instagram AS instagram,
          t.twitter AS twitter
        ) AS redes_social
      ) AS contato,
      -- Status flags
      TRUE AS ativo,
      TRUE AS aberto_ao_publico,
      -- Horario de funcionamento
      CASE
        WHEN t.funciona_24_horas = 1 THEN [STRUCT('Todos os dias' AS dia, TIME(0, 0, 0) AS abre, TIME(23, 59, 59) AS fecha)]
        ELSE 
          ARRAY_CONCAT(
            CASE
              WHEN t.hora_abre IS NOT NULL AND t.hora_fecha IS NOT NULL AND t.hora_abre != 0 AND t.hora_fecha != 0 THEN
                [STRUCT('Segunda a Sexta' as dia, TIME(t.hora_abre, 0, 0) as abre, TIME(t.hora_fecha, 0, 0) as fecha)]
              ELSE CAST([] AS ARRAY<STRUCT<dia STRING, abre TIME, fecha TIME>>)
            END,
            CASE
              WHEN t.hora_abre_sabado IS NOT NULL AND t.hora_fecha_sabado IS NOT NULL AND t.hora_abre_sabado != 0 AND t.hora_fecha_sabado != 0 THEN
                [STRUCT('Sábado' as dia, TIME(t.hora_abre_sabado, 0, 0) as abre, TIME(t.hora_fecha_sabado, 0, 0) as fecha)]
              ELSE CAST([] AS ARRAY<STRUCT<dia STRING, abre TIME, fecha TIME>>)
            END
          )
      END AS horario_funcionamento,
      '{{ref("raw_equipamentos_saude_unidades_arcgis")}}' AS fonte,
      CAST(NULL AS DATE) AS vigencia_inicio,
      CAST(NULL AS DATE) AS vigencia_fim,
      -- Esfera (city, state, or federal level) from estabelecimento join
      e.esfera AS esfera,
      -- Metadata as JSON
      TO_JSON_STRING(STRUCT(
        t.area_planejamento,
        t.email2
      )) AS metadata,
      -- Last update timestamp
      CURRENT_TIMESTAMP() AS updated_at
    FROM
      saude_unidades_processed AS t
    LEFT JOIN
       {{ source("dados_mestres", "bairro") }} as b
    ON 
      ST_CONTAINS(b.geometry, ST_GEOGPOINT(SAFE_CAST(t.longitude_unidade AS FLOAT64), SAFE_CAST(t.latitude_unidade AS FLOAT64)))
    LEFT JOIN
      estabelecimento_esfera AS e
    ON
      t.cnes = e.id_cnes
  )
SELECT
  plus11,
  id_equipamento,
  TRIM(secretaria_responsavel) AS secretaria_responsavel,
  TRIM(tipo_equipamento) AS tipo_equipamento,
  nome_oficial,
  nome_popular,
  plus10,
  plus8,
  plus6,
  latitude,
  longitude,
  geometry,
  endereco,
  bairro,
  contato,
  ativo,
  aberto_ao_publico,
  horario_funcionamento,
  fonte,
  vigencia_inicio,
  vigencia_fim,
  esfera,
  metadata,
  updated_at
FROM
  tb