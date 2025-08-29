
{{
    config(
        alias='identificacao_membro',
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

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nome_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS id_filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior
    --column: ind_filiacao_1_nom_completo_mae_membt
    NULL AS filiacao_1_nom_completo_mae_membt, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS filiacao_1_nome_completo_mae_membt,

    --column: filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS filiacao_2,

    --column: ind_filiacao_1_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS id_filiacao_1_nom_completo_mae_membt,
    --column: ind_filiacao_1_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^0$') THEN 'Opção não marcada no formulário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^1$') THEN 'Opção marcada no formulário'
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS filiacao_1_nom_completo_mae_membt,

    --column: ind_filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS id_filiacao_2,

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    NULL AS nao_sabe_nome_mae, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_pai_membt
    NULL AS nao_sabe_nome_pai, --Essa coluna não esta na versao posterior

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    NULL AS nome_mae, --Essa coluna não esta na versao posterior

    --column: nom_completo_pai_membt
    NULL AS nome_pai, --Essa coluna não esta na versao posterior

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '17'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,406,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,406,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_destino_familia_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,11))
        END AS STRING
    ) AS id_familia_destino_transferencia,

    --column: cod_destino_prefeitura_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,407,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,407,13))
        END AS STRING
    ) AS id_prefeitura_destino_transferencia,

    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia_membro,
    --column: cod_est_cadastral_atual_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia_membro,

    --column: cod_familiar_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,431,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,431,4))
        END AS STRING
    ) AS id_pais_nascimento,

    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,173,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,164,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,164,1))
        END AS STRING
    ) AS sexo,

    --column: dta_nasc_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_nascimento,

    --column: dta_transferencia_membt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia_membro,

    --column: filiacao_1_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,174,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,174,70))
        END AS STRING
    ) AS filiacao_1_nome_completo_mae_membt,

    --column: filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,245,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,245,70))
        END AS STRING
    ) AS filiacao_2,

    --column: ind_filiacao_1_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS id_filiacao_1_nom_completo_mae_membt,
    --column: ind_filiacao_1_nom_completo_mae_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^0$') THEN 'Opção não marcada no formulário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,244,1), r'^1$') THEN 'Opção marcada no formulário'
            ELSE TRIM(SUBSTRING(text,244,1))
        END AS STRING
    ) AS filiacao_1_nom_completo_mae_membt,

    --column: ind_filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,315,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,315,1))
        END AS STRING
    ) AS id_filiacao_2,

    --column: ind_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,1))
        END AS STRING
    ) AS sabe_municipio_nascimento,

    --column: ind_nom_completo_mae_membt
    NULL AS nao_sabe_nome_mae, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_pai_membt
    NULL AS nao_sabe_nome_pai, --Essa coluna não esta na versao posterior

    --column: ind_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,405,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,405,1))
        END AS STRING
    ) AS nao_sabe_pais_nascimento,

    --column: ind_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,1))
        END AS STRING
    ) AS sabe_sigla_uf_nascimento,

    --column: nom_apelido_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_membt
    NULL AS nome_mae, --Essa coluna não esta na versao posterior

    --column: nom_completo_pai_membt
    NULL AS nome_pai, --Essa coluna não esta na versao posterior

    --column: nom_ibge_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,320,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,320,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_memb_t
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,70))
        END AS STRING
    ) AS nome,

    --column: nom_pais_origem_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,363,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,363,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,11))
        END AS STRING
    ) AS nis,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_membt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,317,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,317,2))
        END AS STRING
    ) AS sigla_uf_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '17'

