
{{
    config(
        alias='identificacao_primeira_pessoa',
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

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    NULL AS id_origem_cadastro_pessoa, --Essa coluna não esta na versao posterior
    --column: nu_origem_cadastro_pessoa
    NULL AS origem_cadastro_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    NULL AS filiacao_1_nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: filiacao_2
    NULL AS filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_1nom_completo_mae_pessoa
    NULL AS id_filiacao_1nom_completo_mae_pessoa, --Essa coluna não esta na versao posterior

    --column: ind_filiacao_2
    NULL AS id_filiacao_2, --Essa coluna não esta na versao posterior

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    NULL AS id_identidade_genero, --Essa coluna não esta na versao posterior
    --column: ind_identidade_genero
    NULL AS identidade_genero, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS nao_sabe_nome_mae,

    --column: ind_nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_sabe_nome_pai,

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    NULL AS id_transgenero, --Essa coluna não esta na versao posterior
    --column: ind_transgenero
    NULL AS transgenero, --Essa coluna não esta na versao posterior

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS apelido,

    --column: nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS nome_mae,

    --column: nom_completo_pai_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS nome_pai,

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    NULL AS nome_social_pessoa, --Essa coluna não esta na versao posterior

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    NULL AS id_tipo_identidade_genero, --Essa coluna não esta na versao posterior
    --column: nu_tipo_identidade_genero
    NULL AS tipo_identidade_genero, --Essa coluna não esta na versao posterior

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS filiacao_1_nom_completo_mae_pessoa,

    --column: filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS filiacao_2,

    --column: ind_filiacao_1nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS id_filiacao_1nom_completo_mae_pessoa,

    --column: ind_filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS id_filiacao_2,

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,548,1))
        END AS STRING
    ) AS id_identidade_genero,
    --column: ind_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,548,1))
        END AS STRING
    ) AS identidade_genero,

    --column: ind_nom_completo_mae_pessoa
    NULL AS nao_sabe_nome_mae, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_pai_pessoa
    NULL AS nao_sabe_nome_pai, --Essa coluna não esta na versao posterior

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,549,1))
        END AS STRING
    ) AS id_transgenero,
    --column: ind_transgenero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^1$') THEN 'Sim, a pessoa é trans'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^2$') THEN 'Sim, a pessoa é travesti'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^3$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,549,1))
        END AS STRING
    ) AS transgenero,

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    NULL AS apelido, --Essa coluna não esta na versao posterior

    --column: nom_completo_mae_pessoa
    NULL AS nome_mae, --Essa coluna não esta na versao posterior

    --column: nom_completo_pai_pessoa
    NULL AS nome_pai, --Essa coluna não esta na versao posterior

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS nome_social_pessoa,

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,550,3))
        END AS STRING
    ) AS id_tipo_identidade_genero,
    --column: nu_tipo_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^001$') THEN 'Feminina'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^002$') THEN 'Masculina'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^003$') THEN 'Não binário'
            ELSE TRIM(SUBSTRING(text,550,3))
        END AS STRING
    ) AS tipo_identidade_genero,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '04'

UNION ALL


SELECT

    --column: chv_nat_pes_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,505,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,505,13))
        END AS STRING
    ) AS id_pessoa,

    --column: chv_nat_pes_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,518,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,518,13))
        END AS STRING
    ) AS id_original_pessoa,

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS id_certidao_registrada_cartorio,
    --column: cod_certidao_registrada_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^1$') THEN 'Sim E Tem Certidão De Nascimento E/Ou De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^2$') THEN 'Sim, Mas Não Tem Certidão De Nascimento Nem De Casamento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^3$') THEN 'Não'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,419,1), r'^4$') THEN 'Não Sabe'
            ELSE TRIM(SUBSTRING(text,419,1))
        END AS STRING
    ) AS certidao_registrada_cartorio,

    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_estado_cadastral,
    --column: cod_est_cadastral_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^4$') THEN 'Excluido'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^5$') THEN 'Aguardando Atribuição Nis'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^6$') THEN 'Aguardando Alteração De Caracterização'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS estado_cadastral,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,7))
        END AS STRING
    ) AS id_municipio_nascimento,

    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS id_local_nascimento,
    --column: cod_local_nascimento_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^1$') THEN 'Neste Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^2$') THEN 'Em Outro Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,1), r'^3$') THEN 'Em Outro País'
            ELSE TRIM(SUBSTRING(text,329,1))
        END AS STRING
    ) AS local_nascimento,

    --column: cod_origem_familia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,433,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,433,11))
        END AS STRING
    ) AS id_familia_origem,

    --column: cod_origem_prefeitura_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,420,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,420,13))
        END AS STRING
    ) AS id_prefeitura_origem,

    --column: cod_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,542,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,542,4))
        END AS STRING
    ) AS id_pais_origem,

    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS id_parentesco_responsavel_familia,
    --column: cod_parentesco_rf_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^01$') THEN 'Pessoa Responsável pela Unidade Familiar - RF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^02$') THEN 'Cônjuge ou companheiro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^03$') THEN 'Filho(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^04$') THEN 'Enteado(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^05$') THEN 'Neto(a) ou bisneto(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^06$') THEN 'Pai ou mãe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^07$') THEN 'Sogro(a)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^08$') THEN 'Irmão ou irmã'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^09$') THEN 'Genro ou nora'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^10$') THEN 'Outro parente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,2), r'^11$') THEN 'Não parente'
            ELSE TRIM(SUBSTRING(text,184,2))
        END AS STRING
    ) AS parentesco_responsavel_familia,

    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS id_raca_cor,
    --column: cod_raca_cor_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^1$') THEN 'Branca'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^2$') THEN 'Preta'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^3$') THEN 'Amarela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^4$') THEN 'Parda'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,186,1), r'^5$') THEN 'Indígena'
            ELSE TRIM(SUBSTRING(text,186,1))
        END AS STRING
    ) AS raca_cor,

    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS id_sexo,
    --column: cod_sexo_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^1$') THEN 'Masculino'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,1), r'^2$') THEN 'Feminino'
            ELSE TRIM(SUBSTRING(text,175,1))
        END AS STRING
    ) AS sexo,

    --column: dta_atual_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,8))
        END    ) AS data_ultima_atualizacao,

    --column: dta_cadastramento_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_cadastro,

    --column: dta_nasc_pessoa
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,176,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,176,8))
        END    ) AS data_nascimento,

    --column: filiacao_1_nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,187,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,187,70))
        END AS STRING
    ) AS filiacao_1_nom_completo_mae_pessoa,

    --column: filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,258,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,258,70))
        END AS STRING
    ) AS filiacao_2,

    --column: ind_filiacao_1nom_completo_mae_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,257,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,257,1))
        END AS STRING
    ) AS id_filiacao_1nom_completo_mae_pessoa,

    --column: ind_filiacao_2
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS id_filiacao_2,

    --column: ind_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,375,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,375,1))
        END AS STRING
    ) AS sabe_id_municipio_nascimento,

    --column: ind_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,548,1))
        END AS STRING
    ) AS id_identidade_genero,
    --column: ind_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,548,1))
        END AS STRING
    ) AS identidade_genero,

    --column: ind_nom_completo_mae_pessoa
    NULL AS nao_sabe_nome_mae, --Essa coluna não esta na versao posterior

    --column: ind_nom_completo_pai_pessoa
    NULL AS nao_sabe_nome_pai, --Essa coluna não esta na versao posterior

    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS id_sabe_pais_nascimento,
    --column: ind_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,418,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,418,1))
        END AS STRING
    ) AS sabe_pais_nascimento,

    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_trabalho_infantil,
    --column: ind_trabalho_infantil_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS trabalho_infantil,

    --column: ind_transferencia_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,1))
        END AS STRING
    ) AS foi_transferido,

    --column: ind_transgenero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,549,1))
        END AS STRING
    ) AS id_transgenero,
    --column: ind_transgenero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^1$') THEN 'Sim, a pessoa é trans'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^2$') THEN 'Sim, a pessoa é travesti'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,549,1), r'^3$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,549,1))
        END AS STRING
    ) AS transgenero,

    --column: ind_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,1))
        END AS STRING
    ) AS sabe_uf_nascimento,

    --column: nom_apelido_pessoa
    NULL AS apelido, --Essa coluna não esta na versao posterior

    --column: nom_completo_mae_pessoa
    NULL AS nome_mae, --Essa coluna não esta na versao posterior

    --column: nom_completo_pai_pessoa
    NULL AS nome_pai, --Essa coluna não esta na versao posterior

    --column: nom_ibge_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,333,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,333,35))
        END AS STRING
    ) AS municipio_nascimento,

    --column: nom_origem_alteracao_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,445,60), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,445,60))
        END AS STRING
    ) AS origem_alteracao,

    --column: nom_pais_origem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,376,40), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,376,40))
        END AS STRING
    ) AS pais_nascimento,

    --column: nom_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,70))
        END AS STRING
    ) AS nome,

    --column: nom_social_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,34), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,34))
        END AS STRING
    ) AS nome_social_pessoa,

    --column: nu_nis_original
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,531,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,531,11))
        END AS STRING
    ) AS nis_original,

    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS id_origem_cadastro_pessoa,
    --column: nu_origem_cadastro_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^01$') THEN 'Online'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,546,2), r'^02$') THEN 'APP'
            ELSE TRIM(SUBSTRING(text,546,2))
        END AS STRING
    ) AS origem_cadastro_pessoa,

    --column: nu_tipo_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,550,3))
        END AS STRING
    ) AS id_tipo_identidade_genero,
    --column: nu_tipo_identidade_genero
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^001$') THEN 'Feminina'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^002$') THEN 'Masculina'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,550,3), r'^003$') THEN 'Não binário'
            ELSE TRIM(SUBSTRING(text,550,3))
        END AS STRING
    ) AS tipo_identidade_genero,

    --column: num_membro_fmla
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_familia,

    --column: num_nis_pessoa_atual
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,11))
        END AS STRING
    ) AS nis,

    --column: num_ordem_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,2))
        END AS STRING
    ) AS id_numero_ordem,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_munic_nasc_pessoa
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,330,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,330,2))
        END AS STRING
    ) AS sigla_uf_municipio_nascimento,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '04'

