
{{
    config(
        alias='domicilio',
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

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '02'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS id_forma_abatecimento_agua_domicilio,
    --column: cod_abaste_agua_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^1$') THEN 'Rede Geral De Distribuição'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^2$') THEN 'Poço Ou Nascente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^3$') THEN 'Cisterna'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,1), r'^4$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,49,1))
        END AS STRING
    ) AS forma_abatecimento_agua_domicilio,

    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_possui_agua_encanada_domicilio,
    --column: cod_agua_canalizada_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS possui_agua_encanada_domicilio,

    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS id_possui_banheiro_domicilio,
    --column: cod_banheiro_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^1$') THEN 'Sim'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^2$') THEN 'Não'
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS possui_banheiro_domicilio,

    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS id_calcamento_domicilio,
    --column: cod_calcamento_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^1$') THEN 'Total'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^2$') THEN 'Parcial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,1), r'^3$') THEN 'Não existe'
            ELSE TRIM(SUBSTRING(text,54,1))
        END AS STRING
    ) AS calcamento_domicilio,

    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_destino_lixo_domicilio,
    --column: cod_destino_lixo_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^1$') THEN 'É coletado diretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^2$') THEN 'É coletado indiretamente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^3$') THEN 'É queimado ou enterrado na propriedade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^4$') THEN 'É jogado em terreno baldio ou logradouro (rua, avenida, etc.)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^5$') THEN 'É jogado em rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^6$') THEN 'Tem outro destino'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS destino_lixo_domicilio,

    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS id_escoamento_sanitario_domicilio,
    --column: cod_escoa_sanitario_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^1$') THEN 'Rede coletora de esgoto ou pluvial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^2$') THEN 'Fossa séptica'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^3$') THEN 'Fossa rudimentar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^4$') THEN 'Vala a céu aberto'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^5$') THEN 'Direto para um rio, lago ou mar'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^6$') THEN 'Outra forma'
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS escoamento_sanitario_domicilio,

    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_especie_domicilio,
    --column: cod_especie_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^1$') THEN 'Particular Permanente'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^2$') THEN 'Particular Improvisado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^3$') THEN 'Coletivo'
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS especie_domicilio,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS id_iluminacao_domicilio,
    --column: cod_iluminacao_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^1$') THEN 'Elétrica Com Medidor Próprio'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^2$') THEN 'Elétrica Com Medidor Comunitário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^3$') THEN 'Elétrica Sem Medidor'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^4$') THEN 'Óleo, Querosene Ou Gás'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^5$') THEN 'Vela'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^6$') THEN 'Outra Forma'
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS iluminacao_domicilio,

    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_local_domicilio,
    --column: cod_local_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^1$') THEN 'Urbana'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^2$') THEN 'Rural'
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS local_domicilio,

    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS id_material_domicilio,
    --column: cod_material_domic_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^1$') THEN 'Alvenaria/tijolo com revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^2$') THEN 'Alvenaria/tijolo sem revestimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^3$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^4$') THEN 'Taipa revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^5$') THEN 'Taipa não-revestida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^6$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^7$') THEN 'Palha'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^8$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS material_domicilio,

    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS id_material_piso_domicilio,
    --column: cod_material_piso_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^1$') THEN 'Terra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^2$') THEN 'Cimento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^3$') THEN 'Madeira aproveitada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^4$') THEN 'Madeira aparelhada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^5$') THEN 'Cerâmica, lajota ou pedra'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^6$') THEN 'Carpete'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,46,1), r'^7$') THEN 'Outro material'
            ELSE TRIM(SUBSTRING(text,46,1))
        END AS STRING
    ) AS material_piso_domicilio,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_comodos_domic_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,2))
        END AS INT64
    ) AS quantidade_comodos_domicilio,

    --column: qtd_comodos_dormitorio_fam
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,44,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,44,2))
        END AS INT64
    ) AS quantidade_comodos_dormitorio,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0625'
    AND SUBSTRING(text,38,2) = '02'

