
{{
    config(
        alias=documento_membro,
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

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    NULL AS id_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior
    --column: ind_paud_memb_obto_orig_arq
    NULL AS paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    NULL AS data_oaud_memb_id_obto_rej, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_cancel_receita
    NULL AS data_paud_cpf_cancel_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpf_susp_receita
    NULL AS data_paud_cpf_susp_receita, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_memb
    NULL AS data_paud_cpo_obr_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_mult_memb
    NULL AS data_paud_memb_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_cpf_titular_memb
    NULL AS data_paud_memb_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_eleitor_mult_memb
    NULL AS data_paud_memb_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_ind_obto
    NULL AS data_paud_memb_id_obto, --Essa coluna não esta na versao posterior

    --column: dt_paud_memb_obto_orig_arq
    NULL AS data_paud_memb_obto_orig_arq, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_inv_memb
    NULL AS data_paud_rf_cpf_inv_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_mult_memb
    NULL AS data_paud_rf_cpf_mult_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_cpf_titular_memb
    NULL AS data_paud_rf_cpf_titular_memb, --Essa coluna não esta na versao posterior

    --column: dt_paud_rf_eleitor_mult_memb
    NULL AS data_paud_rf_eleitor_mult_memb, --Essa coluna não esta na versao posterior

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_paud_memb_obto_orig_arq,
    --column: ind_paud_memb_obto_orig_arq
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS paud_memb_obto_orig_arq,

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '14'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: dt_oaud_memb_ind_obto_rej
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,165,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,165,8))
        END    ) AS data_oaud_memb_id_obto_rej,

    --column: dt_paud_cpf_cancel_receita
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,173,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,173,8))
        END    ) AS data_paud_cpf_cancel_receita,

    --column: dt_paud_cpf_susp_receita
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,181,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,181,8))
        END    ) AS data_paud_cpf_susp_receita,

    --column: dt_paud_cpo_obr_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,8))
        END    ) AS data_paud_cpo_obr_memb,

    --column: dt_paud_memb_cpf_mult_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,109,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,109,8))
        END    ) AS data_paud_memb_cpf_mult_memb,

    --column: dt_paud_memb_cpf_titular_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,125,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,125,8))
        END    ) AS data_paud_memb_cpf_titular_memb,

    --column: dt_paud_memb_eleitor_mult_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,141,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,141,8))
        END    ) AS data_paud_memb_eleitor_mult_memb,

    --column: dt_paud_memb_ind_obto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,8))
        END    ) AS data_paud_memb_id_obto,

    --column: dt_paud_memb_obto_orig_arq
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,8))
        END    ) AS data_paud_memb_obto_orig_arq,

    --column: dt_paud_rf_cpf_inv_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,8))
        END    ) AS data_paud_rf_cpf_inv_memb,

    --column: dt_paud_rf_cpf_mult_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,8))
        END    ) AS data_paud_rf_cpf_mult_memb,

    --column: dt_paud_rf_cpf_titular_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,117,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,117,8))
        END    ) AS data_paud_rf_cpf_titular_memb,

    --column: dt_paud_rf_eleitor_mult_memb
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,133,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,133,8))
        END    ) AS data_paud_rf_eleitor_mult_memb,

    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_oaud_indicativo_obito,
    --column: ind_oaud_memb_ind_obto_neg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS oaud_indicativo_obito,

    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_oaud_carteira_trabalho_multiplicidade,
    --column: ind_oaud_memb_mult_ctps
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS oaud_carteira_trabalho_multiplicidade,

    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_oaud_certidao_nascimento_multiplicidade,
    --column: ind_oaud_memb_mult_rcn
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS oaud_certidao_nascimento_multiplicidade,

    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_oaud_rg_multiplicidade,
    --column: ind_oaud_memb_mult_rg
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS oaud_rg_multiplicidade,

    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_osrg_nao_sabe_registrada_cartorio,
    --column: ind_osrg_nao_sabe_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS osrg_nao_sabe_registrada_cartorio,

    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_osrg_registrada_sem_cetidao_civil,
    --column: ind_osrg_sem_certidao_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS osrg_registrada_sem_cetidao_civil,

    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_osrg_nao_registrada_cartorio,
    --column: ind_osrg_sem_registro_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS osrg_nao_registrada_cartorio,

    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_otrn_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS otrn_transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_otrn_transferida_outra_familia,
    --column: ind_otrn_outra_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS otrn_transferida_outra_familia,

    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_otrn_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_pes
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS otrn_transferida_outro_municipio,

    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_paud_cpf_cancelado_base_receita_federal,
    --column: ind_paud_cpf_cancel_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS paud_cpf_cancelado_base_receita_federal,

    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_paud_cpf_suspenso_base_receita_federal,
    --column: ind_paud_cpf_susp_receita
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS paud_cpf_suspenso_base_receita_federal,

    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido,
    --column: ind_paud_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido,

    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_paud_cadatrado_com_certidao_obito,
    --column: ind_paud_mbo_cad_com_cert_obit
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS paud_cadatrado_com_certidao_obito,

    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_paud_cpf_invalido,
    --column: ind_paud_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS paud_cpf_invalido,

    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_paud_cpf_multiplicidade,
    --column: ind_paud_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS paud_cpf_multiplicidade,

    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_paud_cpf_divergencia_titularidade,
    --column: ind_paud_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS paud_cpf_divergencia_titularidade,

    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_invalido,
    --column: ind_paud_memb_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS paud_titulo_eleitor_invalido,

    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_paud_titulo_eleitor_multiplicidade,
    --column: ind_paud_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS paud_titulo_eleitor_multiplicidade,

    --column: ind_paud_memb_obto_orig_arq
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_paud_memb_obto_orig_arq,
    --column: ind_paud_memb_obto_orig_arq
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS paud_memb_obto_orig_arq,

    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_paud_rejeicao_inclusao_atualizacao_dados_cadastrais,
    --column: ind_paud_rejei_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS paud_rejeicao_inclusao_atualizacao_dados_cadastrais,

    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_paud_responsavel_sem_cpf_tiluto_eleitor,
    --column: ind_paud_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS paud_responsavel_sem_cpf_tiluto_eleitor,

    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_invalido,
    --column: ind_paud_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS paud_responsavel_cpf_invalido,

    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_multiplicidade,
    --column: ind_paud_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS paud_responsavel_cpf_multiplicidade,

    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_paud_responsavel_cpf_divergencia_titularidade,
    --column: ind_paud_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS paud_responsavel_cpf_divergencia_titularidade,

    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_invalido,
    --column: ind_paud_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_invalido,

    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_paud_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_paud_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS paud_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_paud_responsavel_idade_inferior_16_anos,
    --column: ind_paud_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS paud_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_pmig_campo_obrigatorio_nao_preenchido,
    --column: ind_pmig_cpo_obr_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS pmig_campo_obrigatorio_nao_preenchido,

    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_pmig_cpf_invalido,
    --column: ind_pmig_memb_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS pmig_cpf_invalido,

    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_cpf_multiplicidade,
    --column: ind_pmig_memb_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_cpf_multiplicidade,

    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_pmig_cpf_divergencia_titularidade,
    --column: ind_pmig_memb_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS pmig_cpf_divergencia_titularidade,

    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_pmig_titulo_eleitor_multiplicidade,
    --column: ind_pmig_memb_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS pmig_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_pmig_responsavel_sem_cpf_titulo_eleitor,
    --column: ind_pmig_rf_cpf_eleitor_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS pmig_responsavel_sem_cpf_titulo_eleitor,

    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_invalido,
    --column: ind_pmig_rf_cpf_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_invalido,

    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_reponsavel_cpf_multiplicidade,
    --column: ind_pmig_rf_cpf_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_reponsavel_cpf_multiplicidade,

    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_pmig_responsavel_cpf_divergencia_titularidade,
    --column: ind_pmig_rf_cpf_titular_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS pmig_responsavel_cpf_divergencia_titularidade,

    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_invalido,
    --column: ind_pmig_rf_eleitor_inv_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_invalido,

    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_pmig_responsavel_titulo_eleitor_multiplicidade,
    --column: ind_pmig_rf_eleitor_mult_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS pmig_responsavel_titulo_eleitor_multiplicidade,

    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_pmig_responsavel_idade_inferior_16_anos,
    --column: ind_pmig_rf_idade_16_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS pmig_responsavel_idade_inferior_16_anos,

    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_pmig_sem_nunhuma_documentacao,
    --column: ind_pmig_sem_doc_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS pmig_sem_nunhuma_documentacao,

    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_ptab_desativacao_inep,
    --column: ind_ptab_desat_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_inep,

    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_ptrn_verficacao_dados_inep,
    --column: ind_ptrn_memb_inep_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS ptrn_verficacao_dados_inep,

    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_ptrn_sem_relacao_parentesco,
    --column: ind_ptrn_memb_parente_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS ptrn_sem_relacao_parentesco,

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
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '14'

