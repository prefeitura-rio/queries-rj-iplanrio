
{{
    config(
        alias=identificacao_controle,
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

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    NULL AS data_integracao_familia, --Essa coluna não esta na versao posterior

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    NULL AS origem_cadastro, --Essa coluna não esta na versao posterior

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    NULL AS data_integracao_familia, --Essa coluna não esta na versao posterior

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    NULL AS data_integracao_familia, --Essa coluna não esta na versao posterior

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    NULL AS data_integracao_familia, --Essa coluna não esta na versao posterior

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    NULL AS data_integracao_familia, --Essa coluna não esta na versao posterior

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1233,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1233,8))
        END    ) AS data_integracao_familia,

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    NULL AS data_integracao_escolaridade, --Essa coluna não esta na versao posterior

    --column: dta_integracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1233,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1233,8))
        END    ) AS data_integracao_familia,

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1241,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1241,8))
        END    ) AS data_integracao_escolaridade,

    --column: dta_integracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1233,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1233,8))
        END    ) AS data_integracao_familia,

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1241,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1241,8))
        END    ) AS data_integracao_escolaridade,

    --column: dta_integracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1233,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1233,8))
        END    ) AS data_integracao_familia,

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    NULL AS cpf_usu_alt_fam, --Essa coluna não esta na versao posterior

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1241,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1241,8))
        END    ) AS data_integracao_escolaridade,

    --column: dta_integracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1233,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1233,8))
        END    ) AS data_integracao_familia,

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    NULL AS quantidade_pes_calc_rnd, --Essa coluna não esta na versao posterior

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '01'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_condicao_cadastro,
    --column: cod_condicao_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Atualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Desatualizado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^3$') THEN 'Por Confirmação'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^4$') THEN 'Não Se Aplica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^5$') THEN 'Por Confirmação Do Usuário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^6$') THEN 'Pelo RF Ao Confirmar O Indicativo De Óbito'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS condicao_cadastro,

    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastro,
    --column: cod_est_cadastral_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastro,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_forma_coleta,
    --column: cod_forma_coleta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Informação Migrada Como Inexistente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Sem Visita Domiciliar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Com Visita Domiciliar'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS forma_coleta,

    --column: cod_ibge_distrito_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,2))
        END AS STRING
    ) AS id_distrito,

    --column: cod_ibge_setor_censo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,4))
        END AS STRING
    ) AS id_setor_censitario,

    --column: cod_ibge_subdistr_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,2))
        END AS STRING
    ) AS id_subdistrito,

    --column: cod_modalidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_modalidade,

    --column: cod_munic_ibge_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,2))
        END AS STRING
    ) AS id_uf,

    --column: cod_munic_ibge_5_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS id_municipio,

    --column: cod_origem_familia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1092,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1092,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1079,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1079,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1120,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1120,10))
        END AS STRING
    ) AS id_unidade_territorial,

    --column: cpf_usu_alt_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1252,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1252,11))
        END AS STRING
    ) AS cpf_usu_alt_fam,

    --column: dat_alteracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_alteracao,

    --column: dat_atualizacao_familia
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1112,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1112,8))
        END    ) AS data_atualizacao,

    --column: dat_cadastramento_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_catrastro,

    --column: des_complemento_adic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,365,75), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,365,75))
        END AS STRING
    ) AS complemento_adicional,

    --column: des_complemento_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,22), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,22))
        END AS STRING
    ) AS complemento,

    --column: dt_cdstr_atual_fmla
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1103,8))
        END    ) AS data_limite_catastro_atual,

    --column: dta_entrevista_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,8))
        END    ) AS data_entrevista,

    --column: dta_integracao_escolaridade_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1241,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1241,8))
        END    ) AS data_integracao_escolaridade,

    --column: dta_integracao_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1233,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1233,8))
        END    ) AS data_integracao_familia,

    --column: filler
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,448,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,448,38))
        END AS STRING
    ) AS filler,

    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS id_alterada_v7,
    --column: flag_fam_alterada_v7
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^0$') THEN 'Família Não Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^1$') THEN 'Família Atualizada Na V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1111,1), r'^2$') THEN 'Família Oriunda Da V7'
            ELSE TRIM(SUBSTRING(text,1111,1))
        END AS STRING
    ) AS alterada_v7,

    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_cadastro_valido,
    --column: ind_cadastro_valido_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^3$') THEN 'Não Se Aplica'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS cadastro_valido,

    --column: ind_formulario_0_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS formulario_0,

    --column: ind_formulario_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS formulario_1,

    --column: ind_formulario_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS formulario_2,

    --column: ind_formulario_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS formulario_suplementar_1,

    --column: ind_formulario_sup2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS formulario_suplementar_2,

    --column: ind_formulario_sup3_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1230,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1230,1))
        END AS STRING
    ) AS formulario_suplementar_3,

    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: nom_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,742,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,742,70))
        END AS STRING
    ) AS entrevistador,

    --column: nom_localidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,76))
        END AS STRING
    ) AS localidade,

    --column: nom_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,76), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,76))
        END AS STRING
    ) AS logradouro,

    --column: nom_tip_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,38))
        END AS STRING
    ) AS tipo_logradouro,

    --column: nom_titulo_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,213,38), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,213,38))
        END AS STRING
    ) AS titulo_logradouro,

    --column: nom_unidade_territorial_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1130,100), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1130,100))
        END AS STRING
    ) AS unidade_territorial,

    --column: nu_origem_cadastro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1231,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1231,2))
        END AS STRING
    ) AS origem_cadastro,

    --column: num_cep_logradouro_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,440,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,440,8))
        END AS STRING
    ) AS cep,

    --column: num_cpf_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,812,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,812,11))
        END AS STRING
    ) AS cpf_entrevistador,

    --column: num_logradouro_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,327,16), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,327,16))
        END AS INT64
    ) AS numero_logradouro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_pes_calc_rnd
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1249,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1249,3))
        END AS INT64
    ) AS quantidade_pes_calc_rnd,

    --column: txt_obs_entrevistador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,823,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,823,256))
        END AS STRING
    ) AS observacoes_entrevistador,

    --column: txt_referencia_local_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,486,256), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,486,256))
        END AS STRING
    ) AS refencia_logradouro,

    --column: vlr_renda_media_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,9))
        END AS STRING
    ) AS valor_renda_media_original,

    --column: vlr_renda_media_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,9), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,9)) AS INT64) / 100
        END AS FLOAT64
    ) AS valor_renda_media,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '01'

