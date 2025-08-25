
{{
    config(
        alias='representante_legal',
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

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '20'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_representante_legal,

    --column: chv_natural_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,807,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,807,13))
        END AS STRING
    ) AS id_representante_legal,

    --column: cod_familiar_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_representante_legal,

    --column: cod_ibge_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,280,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,280,7))
        END AS STRING
    ) AS id_minicipio_nascimento_representante_legal,

    --column: cod_pais_nasicmento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,324,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,324,4))
        END AS STRING
    ) AS id_pais_nascimento_representante_legal,

    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS id_sexo_representante_legal,
    --column: cod_sexo_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,129,1))
        END AS STRING
    ) AS sexo_representante_legal,

    --column: desc_complemento_lograd_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,923,53), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,923,53))
        END AS STRING
    ) AS descricao_complemento_logradouro_representante_legal,

    --column: desc_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,557,250), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,557,250))
        END AS STRING
    ) AS descricao_representacao_legal,

    --column: dta_cadastramento_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro_representante_legal,

    --column: dta_nasc_rl
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,8))
        END    ) AS data_nascimento_representante_legal,

    --column: email_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,50))
        END AS STRING
    ) AS email_representante_legal,

    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS id_obito_acatado_representante_legal,
    --column: ic_obito_acatado_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^S$') THEN 'Óbito Acatado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,380,1), r'^N$') THEN 'Óbito Não Acatado'
            ELSE TRIM(SUBSTRING(text,380,1))
        END AS STRING
    ) AS obito_acatado_representante_legal,

    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS id_obito_representante_legal,
    --column: ic_obito_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^S$') THEN 'Possui Óbito'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,379,1), r'^N$') THEN 'Não Possui'
            ELSE TRIM(SUBSTRING(text,379,1))
        END AS STRING
    ) AS obito_representante_legal,

    --column: ind_nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,208,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,208,1))
        END AS STRING
    ) AS nao_sabe_nome_mae_representante_legal,

    --column: ind_nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,279,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,279,1))
        END AS STRING
    ) AS nao_sabe_nome_pai_representante_legal,

    --column: nom_completo_mae_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,138,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,138,70))
        END AS STRING
    ) AS mae_representante_legal,

    --column: nom_completo_pai_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,209,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,209,70))
        END AS STRING
    ) AS pai_representante_legal,

    --column: nom_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,70))
        END AS STRING
    ) AS representante_legal,

    --column: nome_bairro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,984,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,984,40))
        END AS STRING
    ) AS bairro_representante_legal,

    --column: nome_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,858,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,858,50))
        END AS STRING
    ) AS logradouro_representante_legal,

    --column: nome_municipio_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,35))
        END AS STRING
    ) AS municipio_nascimento_representante_legal,

    --column: nome_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1033,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1033,35))
        END AS STRING
    ) AS municipio_representante_legal,

    --column: nome_pais_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,40))
        END AS STRING
    ) AS pais_nascimento_representante_legal,

    --column: nome_tipo_logradouro_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,820,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,820,38))
        END AS STRING
    ) AS tipo_logradouro_representante_legal,

    --column: num_cep_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,976,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,976,8))
        END AS STRING
    ) AS cep_representante_legal,

    --column: num_cpf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,11))
        END AS STRING
    ) AS cpf_representante_legal,

    --column: num_ddd_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,381,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,381,3))
        END AS STRING
    ) AS contato_ddd_representante_legal,

    --column: num_ddd_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,394,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,394,3))
        END AS STRING
    ) AS contato_ddd_2_representante_legal,

    --column: num_logradouro_rl
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,908,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,908,15))
        END AS INT64
    ) AS numero_logradouro_representante_legal,

    --column: num_nis_pessoa_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,118,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,118,11))
        END AS STRING
    ) AS nis_representante_legal,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_rl_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_interno_representante_legal,

    --column: num_tel_contato_1_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,384,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,384,10))
        END AS STRING
    ) AS contato_telefone_representante_legal,

    --column: num_tel_contato_2_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,397,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,397,10))
        END AS STRING
    ) AS contato_telefone_2_representante_legal,

    --column: numero_municipio_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1024,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1024,9))
        END AS STRING
    ) AS id_municipio_representante_legal,

    --column: sigla_uf_nascimento_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,322,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,322,2))
        END AS STRING
    ) AS sigla_uf_nascimento_representante_legal,

    --column: sigla_uf_rl
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1068,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1068,2))
        END AS STRING
    ) AS sigla_uf_representante_legal,

    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS id_tipo_representacao_legal,
    --column: tipo_representacao_legal
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^01$') THEN 'Curatela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^02$') THEN 'Tutela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,457,100), r'^03$') THEN 'Guarda'
            ELSE TRIM(SUBSTRING(text,457,100))
        END AS STRING
    ) AS tipo_representacao_legal,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '20'

