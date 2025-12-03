{{
    config( alias='cargo',schema='recursos_humanos_ergon_comlurb')
}}
SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(cargo), r'\.0$', '') AS STRING) AS id_cargo,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome,
    SAFE_CAST(TRIM(categoria) AS STRING) AS categoria,
    SAFE_CAST(TRIM(subcategoria) AS STRING) AS subcategoria,
    SAFE_CAST(TRIM(controle_vaga) AS STRING) AS controle_vaga,
    SAFE_CAST(TRIM(e_aglutinador) AS STRING) AS e_aglutinador,
    SAFE_CAST(TRIM(escolaridade) AS STRING) AS escolaridade,
FROM rj-smfp.recursos_humanos_ergon_comlurb_staging.cargo AS t