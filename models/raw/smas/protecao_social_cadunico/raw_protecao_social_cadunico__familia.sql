
{{
    config(
        alias='familia',
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

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    NULL AS data_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: dt_intgo_rsco_scl_vlco_drts_
    NULL AS data_risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    NULL AS id_risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_inseg_alim
    NULL AS risco_inseguranca_alimentar, --Essa coluna não esta na versao posterior

    --column: ind_risco_scl_vlco_drts
    NULL AS id_risco_violacao_direitos, --Essa coluna não esta na versao posterior
    --column: ind_risco_scl_vlco_drts
    NULL AS risco_violacao_direitos, --Essa coluna não esta na versao posterior

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,548,8))
        END    ) AS data_risco_inseguranca_alimentar,

    --column: dt_intgo_rsco_scl_vlco_drts_
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,539,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,539,8))
        END    ) AS data_risco_violacao_direitos,

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS id_risco_inseguranca_alimentar,
    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS risco_inseguranca_alimentar,

    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS id_risco_violacao_direitos,
    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS risco_violacao_direitos,

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,548,8))
        END    ) AS data_risco_inseguranca_alimentar,

    --column: dt_intgo_rsco_scl_vlco_drts_
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,539,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,539,8))
        END    ) AS data_risco_violacao_direitos,

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS id_risco_inseguranca_alimentar,
    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS risco_inseguranca_alimentar,

    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS id_risco_violacao_direitos,
    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS risco_violacao_direitos,

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,548,8))
        END    ) AS data_risco_inseguranca_alimentar,

    --column: dt_intgo_rsco_scl_vlco_drts_
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,539,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,539,8))
        END    ) AS data_risco_violacao_direitos,

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS id_risco_inseguranca_alimentar,
    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS risco_inseguranca_alimentar,

    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS id_risco_violacao_direitos,
    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS risco_violacao_direitos,

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '03'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,526,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,526,12))
        END AS STRING
    ) AS id_cras_creas,

    --column: cod_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,194,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,194,4))
        END AS STRING
    ) AS id_comunidade_quilombola,

    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_familia_indigena,
    --column: cod_familia_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS familia_indigena,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS id_indigena_residente,
    --column: cod_indigena_reside_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,115,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,115,1))
        END AS STRING
    ) AS indigena_residente,

    --column: cod_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,3))
        END AS STRING
    ) AS id_povo_indigena,

    --column: cod_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,116,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,116,6))
        END AS STRING
    ) AS id_reserva_indigena,

    --column: dt_intgo_rsco_scl_inseg_alim
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,548,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,548,8))
        END    ) AS data_risco_inseguranca_alimentar,

    --column: dt_intgo_rsco_scl_vlco_drts_
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,539,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,539,8))
        END    ) AS data_risco_violacao_direitos,

    --column: ind_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,318,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,318,1))
        END AS STRING
    ) AS nao_sabe_comunidade_quilombola,

    --column: ind_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,343,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,343,1))
        END AS STRING
    ) AS nao_tem_despesa_agua_esgoto,

    --column: ind_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,355,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,355,1))
        END AS STRING
    ) AS nao_tem_despesa_alimentacao,

    --column: ind_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,367,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,367,1))
        END AS STRING
    ) AS nao_tem_despesa_aluguel,

    --column: ind_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,337,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,337,1))
        END AS STRING
    ) AS nao_tem_despesa_energia,

    --column: ind_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,349,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,349,1))
        END AS STRING
    ) AS nao_tem_despesa_gas,

    --column: ind_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,373,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,373,1))
        END AS STRING
    ) AS nao_tem_despesa_medicamentos,

    --column: ind_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,361,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,361,1))
        END AS STRING
    ) AS nao_tem_despesa_transporte,

    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS id_familia_quilombola,
    --column: ind_familia_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,193,1))
        END AS STRING
    ) AS familia_quilombola,

    --column: ind_pessoa_inter_0_17_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,325,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,325,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_0_17_anos,

    --column: ind_pessoa_inter_18_59_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_18_59_anos,

    --column: ind_pessoa_inter_60_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,331,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,331,1))
        END AS STRING
    ) AS nao_tem_pessoas_internadas_mais_60_anos,

    --column: ind_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,192,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,192,1))
        END AS STRING
    ) AS nao_sabe_reserva_indigena,

    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS id_risco_inseguranca_alimentar,
    --column: ind_risco_scl_inseg_alim
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,547,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,547,1))
        END AS STRING
    ) AS risco_inseguranca_alimentar,

    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS id_risco_violacao_direitos,
    --column: ind_risco_scl_vlco_drts
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,538,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,538,1))
        END AS STRING
    ) AS risco_violacao_direitos,

    --column: nom_centro_assist_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,456,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,456,70))
        END AS STRING
    ) AS cras_creas,

    --column: nom_comunidade_quilombola_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,198,120), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,198,120))
        END AS STRING
    ) AS comunidade_quilombola,

    --column: nom_estab_assist_saude_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,374,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,374,70))
        END AS STRING
    ) AS estabelecimento_saude,

    --column: nom_povo_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,70))
        END AS STRING
    ) AS povo_indigena,

    --column: nom_reserva_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,122,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,122,70))
        END AS STRING
    ) AS reserva_indigena,

    --column: nu_estbo_saude
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,444,12), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,444,12))
        END AS STRING
    ) AS id_estabelecimento_saude,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_familias_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,321,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,321,2))
        END AS INT64
    ) AS familias_domicilio,

    --column: qtd_pessoa_inter_0_17_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,323,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,323,2))
        END AS INT64
    ) AS pessoas_internadas_0_17_anos,

    --column: qtd_pessoa_inter_18_59_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,326,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,326,2))
        END AS INT64
    ) AS pessoas_internadas_18_59_anos,

    --column: qtd_pessoa_inter_60_anos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,329,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,329,2))
        END AS INT64
    ) AS pessoas_internadas_mais_60_anos,

    --column: qtd_pessoas_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,319,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,319,2))
        END AS INT64
    ) AS pessoas_domicilio,

    --column: val_desp_agua_esgoto_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,338,5))
        END AS STRING
    ) AS despesa_agua_esgoto_original,

    --column: val_desp_agua_esgoto_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,338,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,338,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_agua_esgoto,

    --column: val_desp_alimentacao_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,350,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_alimentacao,

    --column: val_desp_alimentacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,350,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,350,5))
        END AS STRING
    ) AS despesa_alimentacao_original,

    --column: val_desp_aluguel_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,362,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_aluguel,

    --column: val_desp_aluguel_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,362,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,362,5))
        END AS STRING
    ) AS despesa_aluguel_original,

    --column: val_desp_energia_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,332,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_energia,

    --column: val_desp_energia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,332,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,332,5))
        END AS STRING
    ) AS despesa_energia_original,

    --column: val_desp_gas_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,344,5))
        END AS STRING
    ) AS despesa_gas_original,

    --column: val_desp_gas_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,344,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,344,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_gas,

    --column: val_desp_medicamentos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,368,5))
        END AS STRING
    ) AS despesa_medicamentos_original,

    --column: val_desp_medicamentos_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,368,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,368,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_medicamentos,

    --column: val_desp_transpor_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,356,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS despesa_transporte,

    --column: val_desp_transpor_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,356,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,356,5))
        END AS STRING
    ) AS despesa_transporte_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0625'
    AND SUBSTRING(text,38,2) = '03'

