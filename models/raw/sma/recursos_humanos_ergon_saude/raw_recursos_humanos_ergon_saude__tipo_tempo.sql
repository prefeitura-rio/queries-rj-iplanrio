{{
    config(
        alias='tipo_tempo',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(sigla), r'\.0$', '') AS string) AS id_tipo_tempo,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome_tipo_tempo,
    SAFE_CAST(TRIM(aposentadoria) AS BOOLEAN) AS aposentadoria,
    SAFE_CAST(TRIM(ferias) AS BOOLEAN) AS ferias,
    SAFE_CAST(TRIM(dias_fer) AS BOOLEAN) AS dias_de_ferias,
    SAFE_CAST(TRIM(adictserv) AS BOOLEAN) AS trienio,
    SAFE_CAST(TRIM(licesp) AS BOOLEAN) AS licenca_especial,
    SAFE_CAST(TRIM(dias_licesp) AS BOOLEAN) AS dias_de_licenca_especial,
    SAFE_CAST(TRIM(adictchefia) AS BOOLEAN) AS tempo_de_chefia,
    SAFE_CAST(TRIM(progressao) AS BOOLEAN) AS progressao,
FROM {{ source('brutos_ergon_saude_staging', 'TIPO_TEMPO') }} AS t