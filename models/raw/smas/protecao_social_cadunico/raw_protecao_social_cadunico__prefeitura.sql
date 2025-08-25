
{{
    config(
        alias='prefeitura',
        schema='protecao_social_cadunico',
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '98'

UNION ALL


SELECT

    --column: chv_natural_prefeitura
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_munic_ibge_2_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS sigla_uf,

    --column: cod_munic_ibge_5_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,5))
        END AS STRING
    ) AS if_municipio,

    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_migracao,
    --column: ind_migracao_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS migracao,

    --column: nom_prefeitura_pre
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS prefeitura,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '98'

