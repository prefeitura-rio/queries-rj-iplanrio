
{{
    config(
        alias='documento_pessoa',
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

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0625'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0626'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0627'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    NULL AS data_integracao_biometria_cin, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_cnh
    NULL AS data_integracao_biometria_cnh, --Essa coluna não esta na versao posterior

    --column: dt_intgo_biometria_tse
    NULL AS data_integracao_biometria_tse, --Essa coluna não esta na versao posterior

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    NULL AS id_biometria_cin, --Essa coluna não esta na versao posterior
    --column: in_biometria_cin
    NULL AS biometria_cin, --Essa coluna não esta na versao posterior

    --column: in_biometria_cnh
    NULL AS id_biometria_cnh, --Essa coluna não esta na versao posterior
    --column: in_biometria_cnh
    NULL AS biometria_cnh, --Essa coluna não esta na versao posterior

    --column: in_biometria_tse
    NULL AS id_biometria_tse, --Essa coluna não esta na versao posterior
    --column: in_biometria_tse
    NULL AS biometria_tse, --Essa coluna não esta na versao posterior

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0628'
    AND SUBSTRING(text,38,2) = '05'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cartorio_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,183,15), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,183,15))
        END AS STRING
    ) AS id_cartorio_certidao,

    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_certidao_civil,
    --column: cod_certidao_civil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Nascimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^3$') THEN 'RANI'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^4$') THEN 'Óbito'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS certidao_civil,

    --column: cod_complemento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,225,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,225,5))
        END AS STRING
    ) AS id_complemento_rg,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_folha_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,4))
        END AS STRING
    ) AS folha_certidao,

    --column: cod_ibge_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,7))
        END AS STRING
    ) AS id_municipio_certidao,

    --column: cod_livro_termo_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo___matricula__certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,32))
        END AS STRING
    ) AS id_termi_matricula_certidao,

    --column: dt_intgo_biometria_cin
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,8))
        END    ) AS data_integracao_biometria_cin,

    --column: dt_intgo_biometria_cnh
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,353,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,353,8))
        END    ) AS data_integracao_biometria_cnh,

    --column: dt_intgo_biometria_tse
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,8))
        END    ) AS data_integracao_biometria_tse,

    --column: dta_emissao_cart_trab_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,260,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,260,8))
        END    ) AS data_emissao_carteira_trabalho,

    --column: dta_emissao_certid_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,131,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,131,8))
        END    ) AS data_emissao_certidao,

    --column: dta_emissao_ident_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,230,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,230,8))
        END    ) AS data_emissao_rg,

    --column: in_biometria_cin
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS id_biometria_cin,
    --column: in_biometria_cin
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^0$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^1$') THEN 'Sim'
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS biometria_cin,

    --column: in_biometria_cnh
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,352,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,352,1))
        END AS STRING
    ) AS id_biometria_cnh,
    --column: in_biometria_cnh
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,352,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,352,1), r'^0$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,352,1), r'^1$') THEN 'Sim'
            ELSE TRIM(SUBSTRING(text,352,1))
        END AS STRING
    ) AS biometria_cnh,

    --column: in_biometria_tse
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS id_biometria_tse,
    --column: in_biometria_tse
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^0$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^1$') THEN 'Sim'
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS biometria_tse,

    --column: nom_cartorio_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,70))
        END AS STRING
    ) AS cartorio_certidao,

    --column: nom_munic_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,35))
        END AS STRING
    ) AS municipio_certidao,

    --column: num_cart_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,248,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,248,7))
        END AS STRING
    ) AS id_carteira_trabalho,

    --column: num_cpf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,11))
        END AS STRING
    ) AS cpf,

    --column: num_identidade_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,20))
        END AS STRING
    ) AS rg,

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

    --column: num_secao_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,287,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,287,4))
        END AS STRING
    ) AS id_secao_titulo_eleitor,

    --column: num_serie_trab_prev_soc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,255,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,255,5))
        END AS STRING
    ) AS id_serie_carteira_trabalho,

    --column: num_titulo_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,270,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,270,13))
        END AS STRING
    ) AS id_titulo_eleitor,

    --column: num_zona_tit_eleitor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,4))
        END AS STRING
    ) AS id_zona_titulo_eleitor,

    --column: sig_orgao_emissor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,240,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,240,8))
        END AS STRING
    ) AS orgao_emissor_rg,

    --column: sig_uf_cart_trab_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,268,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,268,2))
        END AS STRING
    ) AS sigla_uf_carteira_trabalho,

    --column: sig_uf_certid_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,2))
        END AS STRING
    ) AS sigla_uf_certidao,

    --column: sig_uf_ident_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,2))
        END AS STRING
    ) AS sigla_uf_rg,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0629'
    AND SUBSTRING(text,38,2) = '05'

