
{{
    config(
        alias='contato',
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

    --column: cpf_operador_responsavel
    NULL AS cpf_operador_responsavel, --Essa coluna não esta na versao posterior

    --column: data_arquivos_carregados
    NULL AS data_arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    NULL AS arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    NULL AS cpf_operador_responsavel, --Essa coluna não esta na versao posterior

    --column: data_arquivos_carregados
    NULL AS data_arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    NULL AS arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    NULL AS cpf_operador_responsavel, --Essa coluna não esta na versao posterior

    --column: data_arquivos_carregados
    NULL AS data_arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    NULL AS arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    NULL AS cpf_operador_responsavel, --Essa coluna não esta na versao posterior

    --column: data_arquivos_carregados
    NULL AS data_arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    NULL AS arquivos_carregados, --Essa coluna não esta na versao posterior

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '09'

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

    --column: cpf_operador_responsavel
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,129,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,129,11))
        END AS STRING
    ) AS cpf_operador_responsavel,

    --column: data_arquivos_carregados
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,8))
        END    ) AS data_arquivos_carregados,

    --column: email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,50), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,50))
        END AS STRING
    ) AS email,

    --column: ic_arquivos_carregados
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,120,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,120,1))
        END AS STRING
    ) AS arquivos_carregados,

    --column: ic_envio_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,119,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,119,1))
        END AS STRING
    ) AS autoriza_envio_email,

    --column: ic_envo_sms_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,53,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,53,1))
        END AS STRING
    ) AS contato_envio_sms,

    --column: ic_envo_sms_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,1))
        END AS STRING
    ) AS contato_2_envio_sms,

    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS id_contato_tipo,
    --column: ic_tipo_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,52,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,52,1))
        END AS STRING
    ) AS contato_tipo,

    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS id_contato_2_tipo,
    --column: ic_tipo_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^L$') THEN 'Celular'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^C$') THEN 'Trabalho'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^R$') THEN 'Residencial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^O$') THEN 'Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,66,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Obrigatoriedade Do Campo “Telefone”)'
            ELSE TRIM(SUBSTRING(text,66,1))
        END AS STRING
    ) AS contato_2_tipo,

    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS id_email_tipo,
    --column: ic_tipo_email_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^P$') THEN 'E-Mail Pessoal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^R$') THEN 'E-Mail De Recado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^N$') THEN 'Não Tem'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^D$') THEN 'Não Declarado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,68,1), r'^S$') THEN 'Sem Coleta De Dados (Família Não Alterada/Atualizada Após A Criação Do Campo “E-Mail”)'
            ELSE TRIM(SUBSTRING(text,68,1))
        END AS STRING
    ) AS email_tipo,

    --column: num_ddd_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,2))
        END AS STRING
    ) AS contato_ddd,

    --column: num_ddd_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,54,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,54,2))
        END AS STRING
    ) AS contato_2_ddd,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: num_tel_contato_1_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,42,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,42,10))
        END AS STRING
    ) AS contato_telefone,

    --column: num_tel_contato_2_fam
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,10))
        END AS STRING
    ) AS contato_2_telefone,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '09'

