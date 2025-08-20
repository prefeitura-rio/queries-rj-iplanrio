{{
    config(
        alias="localizacao_obra",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

WITH loc AS (
  SELECT
      SAFE_CAST(
          REGEXP_REPLACE(cd_obra, r'\.0$', '') AS STRING
      ) id_obra,
      SAFE_CAST(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            TRIM(INITCAP(nm_bairro)),
                            'Braz De Pina',
                            'Bras de Pina'
                        ),
                        'Oswaldo Cruz',
                        'Osvaldo Cruz'
                    ) ,
                    'Freguesia(Jacarepagua)',
                    'Freguesia (Jacarepagua)'
                ),
                'Freguesia(Ilha)',
                'Freguesia (Ilha)'
            ),
            'Freguesia (Ilha Do Governador)',
            'Freguesia (Ilha)'
        )
        AS STRING
      ) bairro,
      SAFE_CAST(
        REPLACE(
          TRIM(nm_ap),
          'AP ',
          ''
        ) AS STRING
      ) id_regiao_planejamento,
      SAFE_CAST(
          INITCAP(endereco) AS STRING
      ) endereco,
  FROM {{ source('brutos_siscob_staging', 'localizacao_obra') }}
),

loc_geo AS (
  SELECT
    loc.id_obra,
    bairros.id_bairro,
    bairros.nome AS bairro,
    loc.id_regiao_planejamento,
    loc.endereco,
    end_geo.latitude,
    end_geo.longitude
  FROM loc
  LEFT JOIN {{ source('dados_mestres', 'bairro') }} bairros
    ON UPPER(loc.bairro) = UPPER(REGEXP_REPLACE(NORMALIZE(TRIM(nome), NFD), r'\pM', ''))
  LEFT JOIN {{ source('brutos_siscob_staging', 'enderecos_geolocalizados') }} end_geo
    ON UPPER(loc.endereco) = UPPER(end_geo.address)
)

SELECT
  l.id_obra,
  l.id_bairro,
  l.bairro,
  l.id_regiao_planejamento,
  CASE
      WHEN id_regiao_planejamento = "1.1" THEN "AP 1.1 - Centro"
      WHEN id_regiao_planejamento = "2.1" THEN "AP 2.1 - Zona Sul"
      WHEN id_regiao_planejamento = "2.2" THEN "AP 2.2 - Tijuca"
      WHEN id_regiao_planejamento = "3.1" THEN "AP 3.1 - Ramos"
      WHEN id_regiao_planejamento = "3.2" THEN "AP 3.2 - Meier"
      WHEN id_regiao_planejamento = "3.3" THEN "AP 3.3 - Madureira"
      WHEN id_regiao_planejamento = "3.4" THEN "AP 3.4 - Inhaúma"
      WHEN id_regiao_planejamento = "3.5" THEN "AP 3.5 - Penha"
      WHEN id_regiao_planejamento = "3.6" THEN "AP 3.6 - Pavuna"
      WHEN id_regiao_planejamento = "3.7" THEN "AP 3.7 - Ilha do Governador"
      WHEN id_regiao_planejamento = "4.1" THEN "AP 4.1 - Jacarepaguá"
      WHEN id_regiao_planejamento = "4.2" THEN "AP 4.2 - Barra da Tijuca"
      WHEN id_regiao_planejamento = "5.1" THEN "AP 5.1 - Bangu"
      WHEN id_regiao_planejamento = "5.2" THEN "AP 5.2 - Campo Grande"
      WHEN id_regiao_planejamento = "5.3" THEN "AP 5.3 - Santa Cruz"
      WHEN id_regiao_planejamento = "5.4" THEN "AP 5.4 - Guaratiba"
  ELSE id_regiao_planejamento
  END AS bairro_regiao_planejamento,
  CASE
      WHEN id_regiao_planejamento = "1.1" THEN "-22.907090"
      WHEN id_regiao_planejamento = "2.1" THEN "-22.968931"
      WHEN id_regiao_planejamento = "2.2" THEN "-22.943738"
      WHEN id_regiao_planejamento = "3.1" THEN "-22.858152"
      WHEN id_regiao_planejamento = "3.2" THEN "-22.899457"
      WHEN id_regiao_planejamento = "3.3" THEN "-22.858773"
      WHEN id_regiao_planejamento = "3.4" THEN "-22.867076"
      WHEN id_regiao_planejamento = "3.5" THEN "-22.828848"
      WHEN id_regiao_planejamento = "3.6" THEN "-22.825014"
      WHEN id_regiao_planejamento = "3.7" THEN "-22.803542"
      WHEN id_regiao_planejamento = "4.1" THEN "-22.941678"
      WHEN id_regiao_planejamento = "4.2" THEN "-22.990383"
      WHEN id_regiao_planejamento = "5.1" THEN "-22.869157"
      WHEN id_regiao_planejamento = "5.2" THEN "-22.890034"
      WHEN id_regiao_planejamento = "5.3" THEN "-22.910274"
      WHEN id_regiao_planejamento = "5.4" THEN "-22.96477646089086"
  ELSE NULL
  END AS latitude_regiao_planejamento,
  CASE
      WHEN id_regiao_planejamento = "1.1" THEN "-43.208930"
      WHEN id_regiao_planejamento = "2.1" THEN "-43.201832"
      WHEN id_regiao_planejamento = "2.2" THEN "-43.240806"
      WHEN id_regiao_planejamento = "3.1" THEN "-43.247817"
      WHEN id_regiao_planejamento = "3.2" THEN "-43.280302"
      WHEN id_regiao_planejamento = "3.3" THEN "-43.334166"
      WHEN id_regiao_planejamento = "3.4" THEN "-43.279674"
      WHEN id_regiao_planejamento = "3.5" THEN "-43.294005"
      WHEN id_regiao_planejamento = "3.6" THEN "-43.379834"
      WHEN id_regiao_planejamento = "3.7" THEN "-43.212141"
      WHEN id_regiao_planejamento = "4.1" THEN "-43.360875"
      WHEN id_regiao_planejamento = "4.2" THEN "-43.419226"
      WHEN id_regiao_planejamento = "5.1" THEN "-43.468649"
      WHEN id_regiao_planejamento = "5.2" THEN "-43.566839"
      WHEN id_regiao_planejamento = "5.3" THEN "-43.679449"
      WHEN id_regiao_planejamento = "5.4" THEN "-43.62066799429958"
  ELSE NULL
  END AS longitude_regiao_planejamento,
  l.endereco,
  l.latitude,
  l.longitude,
FROM loc_geo l