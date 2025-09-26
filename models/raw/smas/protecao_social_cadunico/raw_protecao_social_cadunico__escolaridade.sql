
{{
    config(
        alias='escolaridade',
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

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    NULL AS id_origem_dados_escolaridade, --Essa coluna não esta na versao posterior
    --column: cod_origem_dados_escolaridade
    NULL AS origem_dados_escolaridade, --Essa coluna não esta na versao posterior

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    NULL AS data_integracao_escolaridade_membro, --Essa coluna não esta na versao posterior

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_origem_dados_escolaridade,
    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Origem Autodeclarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Origem Presença'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS origem_dados_escolaridade,

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_integracao_escolaridade_membro,

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_origem_dados_escolaridade,
    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Origem Autodeclarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Origem Presença'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS origem_dados_escolaridade,

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_integracao_escolaridade_membro,

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_origem_dados_escolaridade,
    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Origem Autodeclarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Origem Presença'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS origem_dados_escolaridade,

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_integracao_escolaridade_membro,

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '07'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS id_ano_serie_frequenta,
    --column: cod_ano_serie_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^01$') THEN 'Frequenta - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^02$') THEN 'Frequenta - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^03$') THEN 'Frequenta - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^04$') THEN 'Frequenta - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^05$') THEN 'Frequenta - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^06$') THEN 'Frequenta - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^07$') THEN 'Frequenta - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^08$') THEN 'Frequenta - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^09$') THEN 'Frequenta - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,168,2), r'^10$') THEN 'Frequenta - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,168,2))
        END AS STRING
    ) AS ano_serie_frequenta,

    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS id_ultimo_ano_serie_frequentou,
    --column: cod_ano_serie_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^01$') THEN 'Frequentou - Primeiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^02$') THEN 'Frequentou - Segundo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^03$') THEN 'Frequentou - Terceiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^04$') THEN 'Frequentou - Quarto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^05$') THEN 'Frequentou - Quinto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^06$') THEN 'Frequentou - Sexto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^07$') THEN 'Frequentou - Sétimo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^08$') THEN 'Frequentou - Oitavo(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^09$') THEN 'Frequentou - Nono(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,172,2), r'^10$') THEN 'Frequentou - Curso não-seriado'
            ELSE TRIM(SUBSTRING(text,172,2))
        END AS STRING
    ) AS ultimo_ano_serie_frequentou,

    --column: cod_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END AS STRING
    ) AS id_inep_escola,

    --column: cod_concluiu_frequentou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,1))
        END AS STRING
    ) AS id_concluiu_curso_frequentado,

    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS id_curso_frequenta,
    --column: cod_curso_frequenta_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^01$') THEN 'Frequenta - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^02$') THEN 'Frequenta - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^03$') THEN 'Frequenta - Classe de Alfabetização'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^04$') THEN 'Frequenta - Ensino Fundamental regular (duração 8 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^05$') THEN 'Frequenta - Ensino Fundamental regular (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^06$') THEN 'Frequenta - Ensino Fundamental especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^07$') THEN 'Frequenta - Ensino Médio regular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^08$') THEN 'Frequenta - Ensino Médio especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^09$') THEN 'Frequenta - Ensino Fundamental EJA - séries iniciais (Supletivo - 1 a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^10$') THEN 'Frequenta - Ensino Fundamental EJA - séries finais (Supletivo - 5 a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^11$') THEN 'Frequenta - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^12$') THEN 'Frequenta - Alfabetização para adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^13$') THEN 'Frequenta - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,2), r'^14$') THEN 'Frequenta - Pré-vestibular'
            ELSE TRIM(SUBSTRING(text,166,2))
        END AS STRING
    ) AS curso_frequenta,

    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS id_curso_mais_elevado_frequentou,
    --column: cod_curso_frequentou_pessoa_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^01$') THEN 'Frequentou - Creche'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^02$') THEN 'Frequentou - Pré-escola (exceto CA)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^03$') THEN 'Frequentou - Classe de Alfabetização - CA'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^04$') THEN 'Frequentou - Ensino Fundamental 1a a 4a séries, Elementar (Primário), Primeira fase do 1 o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^05$') THEN 'Frequentou - Ensino Fundamental 5a a 8a séries, Médio 1o ciclo (Ginasial), Segunda fase do 1o grau'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^06$') THEN 'Frequentou - Ensino Fundamental (duração 9 anos)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^07$') THEN 'Frequentou - Ensino Fundamental Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^08$') THEN 'Frequentou - Ensino Médio, 2o grau, Médio 2o ciclo (Científico, Clássico, Técnico, Normal)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^09$') THEN 'Frequentou - Ensino Médio Especial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^10$') THEN 'Frequentou - Ensino Fundamental EJA - séries iniciais (Supletivo 1a a 4a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^11$') THEN 'Frequentou - Ensino Fundamental EJA - séries finais (Supletivo 5a a 8a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^12$') THEN 'Frequentou - Ensino Médio EJA (Supletivo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^13$') THEN 'Frequentou - Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^14$') THEN 'Frequentou - Alfabetização para Adultos'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,170,2), r'^15$') THEN 'Frequentou - Nenhum'
            ELSE TRIM(SUBSTRING(text,170,2))
        END AS STRING
    ) AS curso_mais_elevado_frequentou,

    --column: cod_escola_local_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,1))
        END AS STRING
    ) AS id_escola_localizada_municipio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,150,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,150,7))
        END AS STRING
    ) AS id_municipio_escola,

    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_origem_dados_escolaridade,
    --column: cod_origem_dados_escolaridade
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Origem Autodeclarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Origem Presença'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS origem_dados_escolaridade,

    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_sabe_ler_escrever,
    --column: cod_sabe_ler_escrever_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sabe_ler_escrever,

    --column: dta_integracao_escolaridade_membro
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_integracao_escolaridade_membro,

    --column: ind_censo_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,1))
        END AS STRING
    ) AS escola_nao_tem_inep,

    --column: ind_frequenta_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS frequenta_escola,

    --column: nom_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,70))
        END AS STRING
    ) AS escola,

    --column: nom_munic_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,35))
        END AS STRING
    ) AS municipio_escola,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_escola_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,113,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,113,2))
        END AS STRING
    ) AS sigla_uf_escola,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '07'

