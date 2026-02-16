
{{
    config(
        alias='seguranca_alimentar',
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

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0625'
    AND SUBSTRING(text,38,2) = '11'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_cta_energ_ordem_pessoa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_conta_energia_numero_ordem,

    --column: cod_cta_energ_unid_consum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,20), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,20))
        END AS STRING
    ) AS id_conta_energia_unidade_consumidora,

    --column: cod_errad_trab_escravo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_trabalho_escravo,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: ind_min_energ_elet_doacao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,1))
        END AS STRING
    ) AS enegia_doacao,

    --column: ind_min_energ_luz_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,1))
        END AS STRING
    ) AS nao_pagou_instalacao_energia,

    --column: ind_min_energ_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,1))
        END AS STRING
    ) AS energia_nenhum,

    --column: ind_min_energ_tarifa_soc_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,1))
        END AS STRING
    ) AS energia_tarifa_social,

    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS id_ind_parc_mds_fam,
    --column: ind_parc_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^101$') THEN 'Família Cigana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^201$') THEN 'Família Extrativista'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^202$') THEN 'Família De Pescadores Artesanais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^203$') THEN 'Família Pertencente A Comunidade De Terreiro'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^204$') THEN 'Família Ribeirinha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^205$') THEN 'Família De Agricultores Familiares'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^301$') THEN 'Família Assentada Da Reforma Agrária'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^302$') THEN 'Família Beneficiária Do Programa Nacional De Crédito Fundiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^303$') THEN 'Família Acampada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^304$') THEN 'Família Atingida Por Empreendimentos De Infraestrutura'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^305$') THEN 'Família De Preso Do Sistema Carcerário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^306$') THEN 'Família De Catadores De Material Reciclável'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,106,3), r'^000$') THEN 'Nenhuma'
            ELSE TRIM(SUBSTRING(text,106,3))
        END AS STRING
    ) AS ind_parc_mds_fam,

    --column: ind_prog_prohab_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,105,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,105,1))
        END AS STRING
    ) AS nao_beneficiaria_programa_ministerio_cidades,

    --column: ind_sesan_capit_agua_chuva_prod_alim_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS sesan_capitacao_agua_chuva_producao_alimentos,

    --column: ind_sesan_cisterna_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS sesan_cisterna,

    --column: ind_sesan_feira_pop_mds_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS sesan_vende_feira_popular_mds,

    --column: ind_sesan_horta_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS sesan_horta_comunitaria,

    --column: ind_sesan_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS sesan_nenhum,

    --column: ind_sesan_partic_alim_nutricao_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS sesan_participou_curso_alimentacao_nutricao,

    --column: ind_sesan_proj_cart_indigena_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS sesan_projeto_carteira_indigena,

    --column: ind_sesan_receb_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,1))
        END AS STRING
    ) AS sesan_recebe_alimento_paa,

    --column: ind_sesan_receb_cesta_basica_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,1))
        END AS STRING
    ) AS sesan_recebe_cesta_basica,

    --column: ind_sesan_receb_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS sesan_recebe_leite,

    --column: ind_sesan_refei_cozinha_comum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS sesan_refeicao_cozinha_comunitaria,

    --column: ind_sesan_refei_restau_popular_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,1))
        END AS STRING
    ) AS sesan_refeicao_restaurante_popular,

    --column: ind_sesan_vende_alim_paa_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS sesan_vende_alimento_paa,

    --column: ind_sesan_vende_leite_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS sesan_vende_leite,

    --column: ind_snas_abrigo_adultos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,1))
        END AS STRING
    ) AS snas_abrigo_adultos,

    --column: ind_snas_abrigo_crianca_adoles_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,87,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,87,1))
        END AS STRING
    ) AS snas_abrigo_crianca_adolescente,

    --column: ind_snas_abrigo_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS snas_abrigo_idoso,

    --column: ind_snas_abrigo_mulher_vitima_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,1))
        END AS STRING
    ) AS snas_abrigo_mulher_vitima_violencia,

    --column: ind_snas_abrigo_popul_adulta_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,90,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,90,1))
        END AS STRING
    ) AS snas_abrigo_populacao_adulta,

    --column: ind_snas_acomp_social_liberdade_comunidade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_servico_comunitario,

    --column: ind_snas_acomp_social_liberdade_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS snas_adolescente_liberdade_assistida,

    --column: ind_snas_atend_domic_idosos_defic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS snas_atendimento_domiciliar_idoso_deficiente,

    --column: ind_snas_bpc_deficiente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS snas_bpc_deficiente,

    --column: ind_snas_bpc_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS snas_bpc_idoso,

    --column: ind_snas_centro_dia_idoso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS snas_centro_dia_idoso,

    --column: ind_snas_crianca_0_6_anos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,84,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,84,1))
        END AS STRING
    ) AS snas_crianca_0_6_anos,

    --column: ind_snas_enfrenta_violencia_crianca_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS snas_enfrenta_violencia_crianca,

    --column: ind_snas_grupos_idosos_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,1))
        END AS STRING
    ) AS snas_gtupos_idosos,

    --column: ind_snas_habilit_reabilit_deficiencia_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS snas_habilitacao_reabilitacao_deficiencia,

    --column: ind_snas_inclusao_produtiva_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS snas_inclusao_produtiva,

    --column: ind_snas_nenhum_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,104,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,104,1))
        END AS STRING
    ) AS snas_nunhum,

    --column: ind_snas_orientacao_especial_criancas_adol_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS snas_orientacao_especial_crianca_adolescente,

    --column: ind_snas_paif_fam__
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,1))
        END AS STRING
    ) AS snas_programa_atencao_integram_familia,

    --column: ind_snas_peti_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,1))
        END AS STRING
    ) AS snas_programa_erradicacao_trabalho_infantil,

    --column: ind_snas_projovem_adolescente_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,91,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,91,1))
        END AS STRING
    ) AS snas_projovem_adolescente,

    --column: ind_snas_projovem_campo_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,93,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,93,1))
        END AS STRING
    ) AS snas_projovem_campo,

    --column: ind_snas_projovem_trabalhador_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,1))
        END AS STRING
    ) AS snas_projovem_trabalhador,

    --column: ind_snas_projovem_urbano_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,92,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,92,1))
        END AS STRING
    ) AS snas_projovem_urbano,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0626'
    AND SUBSTRING(text,38,2) = '11'

