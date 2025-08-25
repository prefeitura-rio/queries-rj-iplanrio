
{{
    config(
        alias=status_familia,
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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    NULL AS id_oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_oaud_fam_sem_upload_doc
    NULL AS oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    NULL AS id_paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_paud_fam_sem_upload_doc
    NULL AS paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    NULL AS id_oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_oaud_fam_sem_upload_doc
    NULL AS oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    NULL AS id_paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_paud_fam_sem_upload_doc
    NULL AS paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    NULL AS id_oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_oaud_fam_sem_upload_doc
    NULL AS oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    NULL AS id_paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_paud_fam_sem_upload_doc
    NULL AS paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    NULL AS id_oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_oaud_fam_sem_upload_doc
    NULL AS oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    NULL AS id_paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_paud_fam_sem_upload_doc
    NULL AS paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    NULL AS id_oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_oaud_fam_sem_upload_doc
    NULL AS oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    NULL AS id_paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_paud_fam_sem_upload_doc
    NULL AS paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    NULL AS id_oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_oaud_fam_sem_upload_doc
    NULL AS oaud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    NULL AS id_paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior
    --column: ind_paud_fam_sem_upload_doc
    NULL AS paud_sem_upload_arquivo, --Essa coluna não esta na versao posterior

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS id_oaud_sem_upload_arquivo,
    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS oaud_sem_upload_arquivo,

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS id_paud_sem_upload_arquivo,
    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS paud_sem_upload_arquivo,

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS id_oaud_sem_upload_arquivo,
    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS oaud_sem_upload_arquivo,

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS id_paud_sem_upload_arquivo,
    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS paud_sem_upload_arquivo,

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS id_oaud_sem_upload_arquivo,
    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS oaud_sem_upload_arquivo,

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS id_paud_sem_upload_arquivo,
    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS paud_sem_upload_arquivo,

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    NULL AS data_oaud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_prin_fam
    NULL AS data_paud_cpo_obr_prin_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_cpo_obr_sup1_fam
    NULL AS data_paud_cpo_obr_sup1_fam, --Essa coluna não esta na versao posterior

    --column: dt_paud_fam_sem_upload_doc
    NULL AS data_paud_fam_sem_upload_doc, --Essa coluna não esta na versao posterior

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS id_oaud_sem_upload_arquivo,
    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS oaud_sem_upload_arquivo,

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS id_paud_sem_upload_arquivo,
    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS paud_sem_upload_arquivo,

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

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

    --column: dt_oaud_fam_sem_upload_doc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,114,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,114,8))
        END    ) AS data_oaud_fam_sem_upload_doc,

    --column: dt_paud_cpo_obr_prin_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,8))
        END    ) AS data_paud_cpo_obr_prin_fam,

    --column: dt_paud_cpo_obr_sup1_fam
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,8))
        END    ) AS data_paud_cpo_obr_sup1_fam,

    --column: dt_paud_fam_sem_upload_doc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,8))
        END    ) AS data_paud_fam_sem_upload_doc,

    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS id_exclusao_pessoa_cpf_nulo,
    --column: ind_oaud_excl_pess_cpf_nulo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS exclusao_pessoa_cpf_nulo,

    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS id_oaud_sem_upload_arquivo,
    --column: ind_oaud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS oaud_sem_upload_arquivo,

    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS id_sem_unidade_territorial,
    --column: ind_oend_uterrit_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS sem_unidade_territorial,

    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_transferida_este_municipio_familia_existente,
    --column: ind_otrn_exist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS transferida_este_municipio_familia_existente,

    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_transferida_este_municipio_nova_familia,
    --column: ind_otrn_nova_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS transferida_este_municipio_nova_familia,

    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_transferida_outra_familia_mesmo_municipio,
    --column: ind_otrn_outra_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS transferida_outra_familia_mesmo_municipio,

    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_transferida_outro_municipio,
    --column: ind_otrn_outro_mun_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS transferida_outro_municipio,

    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_patl_dados_desatualizados,
    --column: ind_patl_fam_desatual_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS patl_dados_desatualizados,

    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_principal,
    --column: ind_paud_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_principal,

    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,
    --column: ind_paud_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS paud_campo_obrigatorio_nao_preenchido_formulario_suplementar_1,

    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS id_paud_sem_upload_arquivo,
    --column: ind_paud_fam_sem_upload_doc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS paud_sem_upload_arquivo,

    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS id_paud_sem_responsavel_cadastrado,
    --column: ind_paud_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS paud_sem_responsavel_cadastrado,

    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS id_pmds_pendencia_01,
    --column: ind_pmds_pend_01
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS pmds_pendencia_01,

    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS id_pmds_pendencia_02,
    --column: ind_pmds_pend_02
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS pmds_pendencia_02,

    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS id_pmds_pendencia_03,
    --column: ind_pmds_pend_03
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS pmds_pendencia_03,

    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS id_pmds_pendencia_04,
    --column: ind_pmds_pend_04
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,59,1))
        END AS STRING
    ) AS pmds_pendencia_04,

    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS id_pmds_pendencia_05,
    --column: ind_pmds_pend_05
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,60,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,60,1))
        END AS STRING
    ) AS pmds_pendencia_05,

    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS id_pmds_pendencia_06,
    --column: ind_pmds_pend_06
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,61,1))
        END AS STRING
    ) AS pmds_pendencia_06,

    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS id_pmds_pendencia_07,
    --column: ind_pmds_pend_07
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,62,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,62,1))
        END AS STRING
    ) AS pmds_pendencia_07,

    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_pmds_pendencia_08,
    --column: ind_pmds_pend_08
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS pmds_pendencia_08,

    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_pmds_pendencia_09,
    --column: ind_pmds_pend_09
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS pmds_pendencia_09,

    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_pmds_pendencia_10,
    --column: ind_pmds_pend_10
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS pmds_pendencia_10,

    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_pmds_pendencia_11,
    --column: ind_pmds_pend_11
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS pmds_pendencia_11,

    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_pmds_pendencia_12,
    --column: ind_pmds_pend_12
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS pmds_pendencia_12,

    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_pmds_pendencia_13,
    --column: ind_pmds_pend_13
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS pmds_pendencia_13,

    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_pmds_pendencia_14,
    --column: ind_pmds_pend_14
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS pmds_pendencia_14,

    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS id_pmds_pendencia_15,
    --column: ind_pmds_pend_15
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS pmds_pendencia_15,

    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS id_pmds_pendencia_16,
    --column: ind_pmds_pend_16
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,71,1))
        END AS STRING
    ) AS pmds_pendencia_16,

    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS id_pmds_pendencia_17,
    --column: ind_pmds_pend_17
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,72,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,72,1))
        END AS STRING
    ) AS pmds_pendencia_17,

    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS id_pmds_pendencia_18,
    --column: ind_pmds_pend_18
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,73,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,73,1))
        END AS STRING
    ) AS pmds_pendencia_18,

    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS id_pmds_pendencia_19,
    --column: ind_pmds_pend_19
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS pmds_pendencia_19,

    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS id_pmds_pendencia_20,
    --column: ind_pmds_pend_20
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,75,1))
        END AS STRING
    ) AS pmds_pendencia_20,

    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS id_pmds_pendencia_21,
    --column: ind_pmds_pend_21
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS pmds_pendencia_21,

    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS id_pmds_pendencia_22,
    --column: ind_pmds_pend_22
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,77,1))
        END AS STRING
    ) AS pmds_pendencia_22,

    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS id_pmds_pendencia_23,
    --column: ind_pmds_pend_23
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,78,1))
        END AS STRING
    ) AS pmds_pendencia_23,

    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS id_pmds_pendencia_24,
    --column: ind_pmds_pend_24
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,79,1))
        END AS STRING
    ) AS pmds_pendencia_24,

    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS id_pmds_pendencia_25,
    --column: ind_pmds_pend_25
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,80,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,80,1))
        END AS STRING
    ) AS pmds_pendencia_25,

    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS id_pmds_pendencia_26,
    --column: ind_pmds_pend_26
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS pmds_pendencia_26,

    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS id_pmds_pendencia_27,
    --column: ind_pmds_pend_27
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS pmds_pendencia_27,

    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS id_pmds_pendencia_28,
    --column: ind_pmds_pend_28
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS pmds_pendencia_28,

    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS id_pmds_pendencia_29,
    --column: ind_pmds_pend_29
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS pmds_pendencia_29,

    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS id_pmds_pendencia_30,
    --column: ind_pmds_pend_30
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^0$') THEN 'Sem Pendência'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^1$') THEN 'Pendente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^2$') THEN 'Atualizada'
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS pmds_pendencia_30,

    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos,
    --column: ind_pmig_cpo_obr_prin_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos,

    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS id_pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,
    --column: ind_pmig_cpo_obr_sup1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS pmig_campos_obrigatorios_nao_preenchidos_sumeplentar_1,

    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_pmig_sem_responsavel_familiar_cadastrado,
    --column: ind_pmig_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS pmig_sem_responsavel_familiar_cadastrado,

    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_ptab_desativacao_cras_creas,
    --column: ind_ptab_desat_cras_creas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS ptab_desativacao_cras_creas,

    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS id_ptab_desativacao_eas,
    --column: ind_ptab_desat_eas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS ptab_desativacao_eas,

    --column: ind_ptab_desat_quilomb_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS ptab_desativacao_comunidades_quilombolas,

    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_ptab_desativacao_terras_indigenas,
    --column: ind_ptab_desat_terras_indig_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS ptab_desativacao_terras_indigenas,

    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_ptrn_sem_responsavel_familiar_cadastrado,
    --column: ind_ptrn_sem_rf_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^0$') THEN 'Não Existe'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Existe'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS ptrn_sem_responsavel_familiar_cadastrado,

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
    AND SUBSTRING(text,38,2) = '13'

