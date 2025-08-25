
{{
    config(
        alias=renda,
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

    --column: cod_afastado_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_afastado_semana_passada,

    --column: cod_agricultura_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_atividade_extravista,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS origem_valor_aposentadoria,

    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS id_funcao_principal_trabalho,
    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^01$') THEN 'Trabalhador por conta própria (bico, autônomo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^02$') THEN 'Trabalhador temporário em área rural'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^03$') THEN 'Empregado sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^04$') THEN 'Empregado com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^05$') THEN 'Trabalhador doméstico sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^06$') THEN 'Trabalhador doméstico com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^07$') THEN 'Trabalhador não-remunerado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^08$') THEN 'Militar ou servidor público'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^09$') THEN 'Empregador'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^10$') THEN 'Estagiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^11$') THEN 'Aprendiz'
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS funcao_principal_trabalho,

    --column: cod_trabalho_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_trabalho_remunerado_ultimos_12_meses,

    --column: cod_trabalhou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_trabalho_semana_passada,

    --column: dt_intgo_vlr_aposentadoria
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: fx_rnd_val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_outras_rendas,

    --column: fx_rnd_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,2))
        END AS STRING
    ) AS renda_outras_rendas_original,

    --column: fx_rnd_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,2))
        END AS STRING
    ) AS renda_emprego_ultimo_mes_original,

    --column: fx_rnd_val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_emprego_ultimo_mes,

    --column: fx_rnd_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,2))
        END AS STRING
    ) AS renda_aposentadoria_original,

    --column: fx_rnd_val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_aposentadoria,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,2))
        END AS STRING
    ) AS renda_bruta_12_meses_original,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,51,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_bruta_12_meses,

    --column: fx_rnd_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS STRING
    ) AS renda_doacao_original,

    --column: fx_rnd_val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,53,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_doacao,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS renda_pensao_alimenticia_original,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_pensao_alimenticia,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,57,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_seguro_desemprego,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,2))
        END AS STRING
    ) AS renda_seguro_desemprego_original,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

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

    --column: qtd_meses_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '21'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_afastado_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_afastado_semana_passada,

    --column: cod_agricultura_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_atividade_extravista,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS origem_valor_aposentadoria,

    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS id_funcao_principal_trabalho,
    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^01$') THEN 'Trabalhador por conta própria (bico, autônomo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^02$') THEN 'Trabalhador temporário em área rural'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^03$') THEN 'Empregado sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^04$') THEN 'Empregado com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^05$') THEN 'Trabalhador doméstico sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^06$') THEN 'Trabalhador doméstico com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^07$') THEN 'Trabalhador não-remunerado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^08$') THEN 'Militar ou servidor público'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^09$') THEN 'Empregador'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^10$') THEN 'Estagiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^11$') THEN 'Aprendiz'
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS funcao_principal_trabalho,

    --column: cod_trabalho_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_trabalho_remunerado_ultimos_12_meses,

    --column: cod_trabalhou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_trabalho_semana_passada,

    --column: dt_intgo_vlr_aposentadoria
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: fx_rnd_val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_outras_rendas,

    --column: fx_rnd_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,2))
        END AS STRING
    ) AS renda_outras_rendas_original,

    --column: fx_rnd_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,2))
        END AS STRING
    ) AS renda_emprego_ultimo_mes_original,

    --column: fx_rnd_val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_emprego_ultimo_mes,

    --column: fx_rnd_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,2))
        END AS STRING
    ) AS renda_aposentadoria_original,

    --column: fx_rnd_val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_aposentadoria,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,2))
        END AS STRING
    ) AS renda_bruta_12_meses_original,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,51,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_bruta_12_meses,

    --column: fx_rnd_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS STRING
    ) AS renda_doacao_original,

    --column: fx_rnd_val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,53,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_doacao,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS renda_pensao_alimenticia_original,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_pensao_alimenticia,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,57,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_seguro_desemprego,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,2))
        END AS STRING
    ) AS renda_seguro_desemprego_original,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

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

    --column: qtd_meses_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '21'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_afastado_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_afastado_semana_passada,

    --column: cod_agricultura_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_atividade_extravista,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS origem_valor_aposentadoria,

    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS id_funcao_principal_trabalho,
    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^01$') THEN 'Trabalhador por conta própria (bico, autônomo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^02$') THEN 'Trabalhador temporário em área rural'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^03$') THEN 'Empregado sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^04$') THEN 'Empregado com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^05$') THEN 'Trabalhador doméstico sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^06$') THEN 'Trabalhador doméstico com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^07$') THEN 'Trabalhador não-remunerado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^08$') THEN 'Militar ou servidor público'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^09$') THEN 'Empregador'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^10$') THEN 'Estagiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^11$') THEN 'Aprendiz'
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS funcao_principal_trabalho,

    --column: cod_trabalho_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_trabalho_remunerado_ultimos_12_meses,

    --column: cod_trabalhou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_trabalho_semana_passada,

    --column: dt_intgo_vlr_aposentadoria
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: fx_rnd_val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_outras_rendas,

    --column: fx_rnd_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,2))
        END AS STRING
    ) AS renda_outras_rendas_original,

    --column: fx_rnd_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,2))
        END AS STRING
    ) AS renda_emprego_ultimo_mes_original,

    --column: fx_rnd_val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_emprego_ultimo_mes,

    --column: fx_rnd_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,2))
        END AS STRING
    ) AS renda_aposentadoria_original,

    --column: fx_rnd_val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_aposentadoria,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,2))
        END AS STRING
    ) AS renda_bruta_12_meses_original,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,51,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_bruta_12_meses,

    --column: fx_rnd_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS STRING
    ) AS renda_doacao_original,

    --column: fx_rnd_val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,53,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_doacao,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS renda_pensao_alimenticia_original,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_pensao_alimenticia,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,57,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_seguro_desemprego,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,2))
        END AS STRING
    ) AS renda_seguro_desemprego_original,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

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

    --column: qtd_meses_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '21'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_afastado_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_afastado_semana_passada,

    --column: cod_agricultura_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_atividade_extravista,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS origem_valor_aposentadoria,

    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS id_funcao_principal_trabalho,
    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^01$') THEN 'Trabalhador por conta própria (bico, autônomo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^02$') THEN 'Trabalhador temporário em área rural'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^03$') THEN 'Empregado sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^04$') THEN 'Empregado com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^05$') THEN 'Trabalhador doméstico sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^06$') THEN 'Trabalhador doméstico com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^07$') THEN 'Trabalhador não-remunerado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^08$') THEN 'Militar ou servidor público'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^09$') THEN 'Empregador'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^10$') THEN 'Estagiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^11$') THEN 'Aprendiz'
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS funcao_principal_trabalho,

    --column: cod_trabalho_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_trabalho_remunerado_ultimos_12_meses,

    --column: cod_trabalhou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_trabalho_semana_passada,

    --column: dt_intgo_vlr_aposentadoria
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: fx_rnd_val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_outras_rendas,

    --column: fx_rnd_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,2))
        END AS STRING
    ) AS renda_outras_rendas_original,

    --column: fx_rnd_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,2))
        END AS STRING
    ) AS renda_emprego_ultimo_mes_original,

    --column: fx_rnd_val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_emprego_ultimo_mes,

    --column: fx_rnd_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,2))
        END AS STRING
    ) AS renda_aposentadoria_original,

    --column: fx_rnd_val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_aposentadoria,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,2))
        END AS STRING
    ) AS renda_bruta_12_meses_original,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,51,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_bruta_12_meses,

    --column: fx_rnd_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS STRING
    ) AS renda_doacao_original,

    --column: fx_rnd_val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,53,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_doacao,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS renda_pensao_alimenticia_original,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_pensao_alimenticia,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,57,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_seguro_desemprego,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,2))
        END AS STRING
    ) AS renda_seguro_desemprego_original,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

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

    --column: qtd_meses_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '21'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_afastado_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_afastado_semana_passada,

    --column: cod_agricultura_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_atividade_extravista,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS origem_valor_aposentadoria,

    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS id_funcao_principal_trabalho,
    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^01$') THEN 'Trabalhador por conta própria (bico, autônomo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^02$') THEN 'Trabalhador temporário em área rural'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^03$') THEN 'Empregado sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^04$') THEN 'Empregado com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^05$') THEN 'Trabalhador doméstico sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^06$') THEN 'Trabalhador doméstico com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^07$') THEN 'Trabalhador não-remunerado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^08$') THEN 'Militar ou servidor público'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^09$') THEN 'Empregador'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^10$') THEN 'Estagiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^11$') THEN 'Aprendiz'
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS funcao_principal_trabalho,

    --column: cod_trabalho_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_trabalho_remunerado_ultimos_12_meses,

    --column: cod_trabalhou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_trabalho_semana_passada,

    --column: dt_intgo_vlr_aposentadoria
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: fx_rnd_val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_outras_rendas,

    --column: fx_rnd_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,2))
        END AS STRING
    ) AS renda_outras_rendas_original,

    --column: fx_rnd_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,2))
        END AS STRING
    ) AS renda_emprego_ultimo_mes_original,

    --column: fx_rnd_val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_emprego_ultimo_mes,

    --column: fx_rnd_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,2))
        END AS STRING
    ) AS renda_aposentadoria_original,

    --column: fx_rnd_val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_aposentadoria,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,2))
        END AS STRING
    ) AS renda_bruta_12_meses_original,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,51,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_bruta_12_meses,

    --column: fx_rnd_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS STRING
    ) AS renda_doacao_original,

    --column: fx_rnd_val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,53,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_doacao,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS renda_pensao_alimenticia_original,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_pensao_alimenticia,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,57,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_seguro_desemprego,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,2))
        END AS STRING
    ) AS renda_seguro_desemprego_original,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

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

    --column: qtd_meses_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '21'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura,

    --column: cod_afastado_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,41,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,41,1))
        END AS STRING
    ) AS id_afastado_semana_passada,

    --column: cod_agricultura_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,1))
        END AS STRING
    ) AS id_atividade_extravista,

    --column: cod_familiar_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia,

    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,69,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,63,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,63,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,65,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS origem_valor_aposentadoria,

    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS id_funcao_principal_trabalho,
    --column: cod_principal_trab_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^01$') THEN 'Trabalhador por conta própria (bico, autônomo)'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^02$') THEN 'Trabalhador temporário em área rural'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^03$') THEN 'Empregado sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^04$') THEN 'Empregado com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^05$') THEN 'Trabalhador doméstico sem carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^06$') THEN 'Trabalhador doméstico com carteira de trabalho assinada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^07$') THEN 'Trabalhador não-remunerado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^08$') THEN 'Militar ou servidor público'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^09$') THEN 'Empregador'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^10$') THEN 'Estagiário'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,43,2), r'^11$') THEN 'Aprendiz'
            ELSE TRIM(SUBSTRING(text,43,2))
        END AS STRING
    ) AS funcao_principal_trabalho,

    --column: cod_trabalho_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_trabalho_remunerado_ultimos_12_meses,

    --column: cod_trabalhou_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,1))
        END AS STRING
    ) AS id_trabalho_semana_passada,

    --column: dt_intgo_vlr_aposentadoria
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,86,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,86,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,78,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,78,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: fx_rnd_val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_outras_rendas,

    --column: fx_rnd_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,2))
        END AS STRING
    ) AS renda_outras_rendas_original,

    --column: fx_rnd_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,2))
        END AS STRING
    ) AS renda_emprego_ultimo_mes_original,

    --column: fx_rnd_val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_emprego_ultimo_mes,

    --column: fx_rnd_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,2))
        END AS STRING
    ) AS renda_aposentadoria_original,

    --column: fx_rnd_val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_aposentadoria,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,2))
        END AS STRING
    ) AS renda_bruta_12_meses_original,

    --column: fx_rnd_val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,51,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_bruta_12_meses,

    --column: fx_rnd_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS STRING
    ) AS renda_doacao_original,

    --column: fx_rnd_val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,53,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_doacao,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS renda_pensao_alimenticia_original,

    --column: fx_rnd_val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_pensao_alimenticia,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,57,2)) AS INT64) / 1
        END AS FLOAT64
    ) AS renda_seguro_desemprego,

    --column: fx_rnd_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,57,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,57,2))
        END AS STRING
    ) AS renda_seguro_desemprego_original,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,47,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,47,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

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

    --column: qtd_meses_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '21'

