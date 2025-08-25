
{{
    config(
        alias='trabalho_remuneracao',
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
    NULL AS id_origem_valor_outra_fonte, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_outra_fonte_renda
    NULL AS origem_valor_outra_fonte, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_pensao_ali
    NULL AS id_origem_valor_pensao_alimenticia, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_pensao_ali
    NULL AS origem_valor_pensao_alimenticia, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    NULL AS id_origem_valor_rendimento_bruto, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    NULL AS origem_valor_rendimento_bruto, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_rndmo_mes_passado
    NULL AS id_origem_valor_rendimento_mes_passado, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_rndmo_mes_passado
    NULL AS origem_valor_rendimento_mes_passado, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_seguro_desemprego
    NULL AS id_origem_valor_seguro_desemprego, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_seguro_desemprego
    NULL AS origem_valor_seguro_desemprego, --Essa coluna não esta na versao posterior

    --column: cod_origem_valor_ajuda_doacao
    NULL AS id_origem_valor_ajuda_doacao, --Essa coluna não esta na versao posterior
    --column: cod_origem_valor_ajuda_doacao
    NULL AS origem_valor_ajuda_doacao, --Essa coluna não esta na versao posterior

    --column: cod_origem_valor_aposentadoria
    NULL AS id_origem_valor_aposentadoria, --Essa coluna não esta na versao posterior
    --column: cod_origem_valor_aposentadoria
    NULL AS origem_valor_aposentadoria, --Essa coluna não esta na versao posterior

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
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
    NULL AS data_integracao_aposentadoria, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_outra_fonte_rnda
    NULL AS data_integracao_outras_fonte, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_bruto
    NULL AS data_integracao_renda_bruta_12_meses, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_mes_passado
    NULL AS data_integracao_emprego_ultimo_mes, --Essa coluna não esta na versao posterior

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,83,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,5))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,5))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,65,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,5))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,5))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,54,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,5))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,77,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,5))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,71,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '08'

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
    NULL AS id_origem_valor_outra_fonte, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_outra_fonte_renda
    NULL AS origem_valor_outra_fonte, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_pensao_ali
    NULL AS id_origem_valor_pensao_alimenticia, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_pensao_ali
    NULL AS origem_valor_pensao_alimenticia, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    NULL AS id_origem_valor_rendimento_bruto, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    NULL AS origem_valor_rendimento_bruto, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_rndmo_mes_passado
    NULL AS id_origem_valor_rendimento_mes_passado, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_rndmo_mes_passado
    NULL AS origem_valor_rendimento_mes_passado, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_seguro_desemprego
    NULL AS id_origem_valor_seguro_desemprego, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_seguro_desemprego
    NULL AS origem_valor_seguro_desemprego, --Essa coluna não esta na versao posterior

    --column: cod_origem_valor_ajuda_doacao
    NULL AS id_origem_valor_ajuda_doacao, --Essa coluna não esta na versao posterior
    --column: cod_origem_valor_ajuda_doacao
    NULL AS origem_valor_ajuda_doacao, --Essa coluna não esta na versao posterior

    --column: cod_origem_valor_aposentadoria
    NULL AS id_origem_valor_aposentadoria, --Essa coluna não esta na versao posterior
    --column: cod_origem_valor_aposentadoria
    NULL AS origem_valor_aposentadoria, --Essa coluna não esta na versao posterior

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
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
    NULL AS data_integracao_aposentadoria, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_outra_fonte_rnda
    NULL AS data_integracao_outras_fonte, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_bruto
    NULL AS data_integracao_renda_bruta_12_meses, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_mes_passado
    NULL AS data_integracao_emprego_ultimo_mes, --Essa coluna não esta na versao posterior

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,50,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,50,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,64,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,64,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,83,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,5))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,5))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,65,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,65,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,65,5))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,5))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,54,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,5))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,59,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,77,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,77,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,77,5))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,71,5)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,5))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '08'

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
    NULL AS id_origem_valor_outra_fonte, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_outra_fonte_renda
    NULL AS origem_valor_outra_fonte, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_pensao_ali
    NULL AS id_origem_valor_pensao_alimenticia, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_pensao_ali
    NULL AS origem_valor_pensao_alimenticia, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    NULL AS id_origem_valor_rendimento_bruto, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    NULL AS origem_valor_rendimento_bruto, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_rndmo_mes_passado
    NULL AS id_origem_valor_rendimento_mes_passado, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_rndmo_mes_passado
    NULL AS origem_valor_rendimento_mes_passado, --Essa coluna não esta na versao posterior

    --column: cod_orgm_vlr_seguro_desemprego
    NULL AS id_origem_valor_seguro_desemprego, --Essa coluna não esta na versao posterior
    --column: cod_orgm_vlr_seguro_desemprego
    NULL AS origem_valor_seguro_desemprego, --Essa coluna não esta na versao posterior

    --column: cod_origem_valor_ajuda_doacao
    NULL AS id_origem_valor_ajuda_doacao, --Essa coluna não esta na versao posterior
    --column: cod_origem_valor_ajuda_doacao
    NULL AS origem_valor_ajuda_doacao, --Essa coluna não esta na versao posterior

    --column: cod_origem_valor_aposentadoria
    NULL AS id_origem_valor_aposentadoria, --Essa coluna não esta na versao posterior
    --column: cod_origem_valor_aposentadoria
    NULL AS origem_valor_aposentadoria, --Essa coluna não esta na versao posterior

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
    NULL AS data_integracao_aposentadoria, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_outra_fonte_rnda
    NULL AS data_integracao_outras_fonte, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_bruto
    NULL AS data_integracao_renda_bruta_12_meses, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_mes_passado
    NULL AS data_integracao_emprego_ultimo_mes, --Essa coluna não esta na versao posterior

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
    NULL AS data_integracao_aposentadoria, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_outra_fonte_rnda
    NULL AS data_integracao_outras_fonte, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_bruto
    NULL AS data_integracao_renda_bruta_12_meses, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_mes_passado
    NULL AS data_integracao_emprego_ultimo_mes, --Essa coluna não esta na versao posterior

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
    NULL AS data_integracao_aposentadoria, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_outra_fonte_rnda
    NULL AS data_integracao_outras_fonte, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_bruto
    NULL AS data_integracao_renda_bruta_12_meses, --Essa coluna não esta na versao posterior

    --column: dt_intgo_vlr_rndmo_mes_passado
    NULL AS data_integracao_emprego_ultimo_mes, --Essa coluna não esta na versao posterior

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,127,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,127,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,127,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,127,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,127,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,127,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,127,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,127,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,127,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,127,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '08'

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS id_origem_valor_outra_fonte,
    --column: cod_orgm_vlr_outra_fonte_renda
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,102,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,102,1))
        END AS STRING
    ) AS origem_valor_outra_fonte,

    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS id_origem_valor_pensao_alimenticia,
    --column: cod_orgm_vlr_pensao_ali
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,101,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,101,1))
        END AS STRING
    ) AS origem_valor_pensao_alimenticia,

    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_bruto,
    --column: cod_orgm_vlr_rndmo_bruto_prdo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,97,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,97,1))
        END AS STRING
    ) AS origem_valor_rendimento_bruto,

    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS id_origem_valor_rendimento_mes_passado,
    --column: cod_orgm_vlr_rndmo_mes_passado
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,96,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,96,1))
        END AS STRING
    ) AS origem_valor_rendimento_mes_passado,

    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS id_origem_valor_seguro_desemprego,
    --column: cod_orgm_vlr_seguro_desemprego
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,100,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,100,1))
        END AS STRING
    ) AS origem_valor_seguro_desemprego,

    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS id_origem_valor_ajuda_doacao,
    --column: cod_origem_valor_ajuda_doacao
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,98,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,98,1))
        END AS STRING
    ) AS origem_valor_ajuda_doacao,

    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,99,1))
        END AS STRING
    ) AS id_origem_valor_aposentadoria,
    --column: cod_origem_valor_aposentadoria
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^1$') THEN 'Auto Declarada'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,99,1), r'^2$') THEN 'CNIS'
            ELSE TRIM(SUBSTRING(text,99,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,8))
        END    ) AS data_integracao_aposentadoria,

    --column: dt_intgo_vlr_outra_fonte_rnda
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,127,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,127,8))
        END    ) AS data_integracao_outras_fonte,

    --column: dt_intgo_vlr_rndmo_bruto
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,111,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,111,8))
        END    ) AS data_integracao_renda_bruta_12_meses,

    --column: dt_intgo_vlr_rndmo_mes_passado
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,8))
        END    ) AS data_integracao_emprego_ultimo_mes,

    --column: ind_val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,95,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,95,1))
        END AS STRING
    ) AS nao_recebe_outras_fontes,

    --column: ind_val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,1))
        END AS STRING
    ) AS nao_recebe_remuneracao,

    --column: ind_val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,74,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,74,1))
        END AS STRING
    ) AS nao_recebe_aposentadoria,

    --column: ind_val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS nao_recebe_doacao,

    --column: ind_val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,88,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,88,1))
        END AS STRING
    ) AS nao_recebe_pensao_alimenticia,

    --column: ind_val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,81,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,81,1))
        END AS STRING
    ) AS nao_recebe_seguro_desemprego,

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
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,2))
        END AS INT64
    ) AS meses_trabalhados_nos_ultimos_12,

    --column: val_outras_rendas_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,89,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS outras_fontes,

    --column: val_outras_rendas_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,89,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,89,6))
        END AS STRING
    ) AS outras_fontes_original,

    --column: val_remuner_emprego_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,45,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao,

    --column: val_remuner_emprego_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,45,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,45,6))
        END AS STRING
    ) AS remuneracao_original,

    --column: val_renda_aposent_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,68,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS aposentadoria,

    --column: val_renda_aposent_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,6))
        END AS STRING
    ) AS aposentadoria_original,

    --column: val_renda_bruta_12_meses_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,55,6))
        END AS STRING
    ) AS remuneracao_bruta_original,

    --column: val_renda_bruta_12_meses_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,55,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,55,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS remuneracao_bruta,

    --column: val_renda_doacao_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,6))
        END AS STRING
    ) AS doacoes_original,

    --column: val_renda_doacao_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,61,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS doacoes,

    --column: val_renda_pensao_alimen_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,82,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS pensao_alimenticia,

    --column: val_renda_pensao_alimen_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,82,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,82,6))
        END AS STRING
    ) AS pensao_alimenticia_original,

    --column: val_renda_seguro_desemp_memb
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE SAFE_CAST( TRIM(SUBSTRING(text,75,6)) AS INT64) / 1
        END AS FLOAT64
    ) AS seguro_desemprego,

    --column: val_renda_seguro_desemp_memb
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,6), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,6))
        END AS STRING
    ) AS seguro_desemprego_original,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '08'

