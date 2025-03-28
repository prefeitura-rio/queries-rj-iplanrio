{{
    config(
        alias='documento',
        schema='adm_processo_interno_processorio',
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}



SELECT
    SAFE_CAST(REGEXP_REPLACE(ano_emissao, r'\.0$', '') AS	INT64) AS 	ano_emissao,
    -- SAFE_CAST(assinatura_blob_doc AS STRING) AS assinatura_blob_documento,
    SAFE_CAST(chave_doc AS STRING) AS chave_dococumento,
    SAFE_CAST(conteudo_tp_doc AS STRING) AS conteudo_tp_documento,
    SAFE_CAST(descr_documento AS STRING) AS descricao_documento,
    SAFE_CAST(descr_documento_ai AS STRING) AS descricao_documento_ai,
    SAFE_CAST(dnm_acesso AS STRING) AS denominacao_acesso,
    SAFE_CAST(dnm_id_nivel_acesso AS STRING) AS dnm_id_nivel_acesso,
    SAFE_CAST(dsc_class_doc AS STRING) AS dsc_classificacao_documento,

    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",dt_doc) AS datetime) AS data_documento,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",dt_doc_original) AS datetime) AS data_documento_original,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",dt_finalizacao) AS datetime) AS data_finalizacao,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",dt_primeiraassinatura) AS datetime) AS data_primeiraassinatura,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",dt_reg_doc) AS datetime) AS data_reg_documento,

    SAFE_CAST(fg_eletronico AS STRING) AS fg_eletronico,
    SAFE_CAST(fg_pessoal AS STRING) AS fg_pessoal,
    SAFE_CAST(his_dt_alt AS STRING) AS his_dt_alt,

    SAFE_CAST(REGEXP_REPLACE(id_cadastrante, r'\.0$', '') AS	STRING) AS 	id_cadastrante,
    SAFE_CAST(REGEXP_REPLACE(id_classificacao, r'\.0$', '') AS	STRING) AS 	id_classificacao,
    SAFE_CAST(REGEXP_REPLACE(id_destinatario, r'\.0$', '') AS	STRING) AS 	id_destinatario,
    SAFE_CAST(REGEXP_REPLACE(id_doc, r'\.0$', '') AS	STRING) AS 	id_documento,
    SAFE_CAST(REGEXP_REPLACE(id_doc_anterior, r'\.0$', '') AS	STRING) AS 	id_documento_anterior,
    SAFE_CAST(REGEXP_REPLACE(id_doc_pai, r'\.0$', '') AS	STRING) AS 	id_documento_pai,
    SAFE_CAST(REGEXP_REPLACE(id_forma_doc, r'\.0$', '') AS	STRING) AS 	id_forma_documento,
    SAFE_CAST(REGEXP_REPLACE(id_lota_cadastrante, r'\.0$', '') AS	STRING) AS 	id_lotacao_cadastrante,
    SAFE_CAST(REGEXP_REPLACE(id_lota_destinatario, r'\.0$', '') AS	STRING) AS 	id_lotacao_destinatario,
    SAFE_CAST(REGEXP_REPLACE(id_lota_subscritor, r'\.0$', '') AS	STRING) AS 	id_lotacao_subscritor,
    SAFE_CAST(REGEXP_REPLACE(id_lota_titular, r'\.0$', '') AS	STRING) AS 	id_lotacao_titular,
    SAFE_CAST(REGEXP_REPLACE(id_mob_autuado, r'\.0$', '') AS	STRING) AS 	id_mob_autuado,
    SAFE_CAST(REGEXP_REPLACE(id_mob_pai, r'\.0$', '') AS	STRING) AS 	id_mob_pai,
    SAFE_CAST(REGEXP_REPLACE(id_mod, r'\.0$', '') AS	STRING) AS 	id_mod,
    SAFE_CAST(REGEXP_REPLACE(id_nivel_acesso, r'\.0$', '') AS	STRING) AS 	id_nivel_acesso,
    SAFE_CAST(REGEXP_REPLACE(id_orgao, r'\.0$', '') AS	STRING) AS 	id_orgao,
    SAFE_CAST(REGEXP_REPLACE(id_orgao_destinatario, r'\.0$', '') AS	STRING) AS 	id_orgao_destinatario,
    SAFE_CAST(REGEXP_REPLACE(id_orgao_usu, r'\.0$', '') AS	STRING) AS 	id_orgao_usu,
    SAFE_CAST(REGEXP_REPLACE(id_protocolo, r'\.0$', '') AS	STRING) AS 	id_protocolo,
    SAFE_CAST(REGEXP_REPLACE(id_subscritor, r'\.0$', '') AS	STRING) AS 	id_subscritor,
    SAFE_CAST(REGEXP_REPLACE(id_titular, r'\.0$', '') AS	STRING) AS 	id_titular,
    SAFE_CAST(REGEXP_REPLACE(id_tp_doc, r'\.0$', '') AS	STRING) AS 	id_tipo_documento,

    SAFE_CAST(nm_arq_doc AS STRING) AS nome_arquivo_documento,
    SAFE_CAST(nm_destinatario AS STRING) AS nome_destinatario,
    SAFE_CAST(nm_funcao_subscritor AS STRING) AS nome_funcao_subscritor,
    SAFE_CAST(nm_orgao_destinatario AS STRING) AS nome_orgao_destinatario,
    SAFE_CAST(nm_subscritor_ext AS STRING) AS nome_subscritor_externo,

    SAFE_CAST(REGEXP_REPLACE(num_antigo_doc, r'\.0$', '') AS	INT64) AS 	numero_documento_antigo,
    SAFE_CAST(REGEXP_REPLACE(num_aux_doc, r'\.0$', '') AS	INT64) AS 	numero_documento_auxiliar,
    SAFE_CAST(REGEXP_REPLACE(num_expediente, r'\.0$', '') AS	INT64) AS 	numero_expediente,
    SAFE_CAST(REGEXP_REPLACE(num_ext_doc, r'\.0$', '') AS	INT64) AS 	numero_documento_externo,
    SAFE_CAST(REGEXP_REPLACE(num_paginas, r'\.0$', '') AS	INT64) AS 	numero_paginas,
    SAFE_CAST(REGEXP_REPLACE(num_sequencia, r'\.0$', '') AS	INT64) AS 	numero_sequencia,
    SAFE_CAST(REGEXP_REPLACE(num_via_doc_pai, r'\.0$', '') AS	INT64) AS 	numero_via_documento_pai,

    SAFE_CAST(obs_orgao_doc AS STRING) AS observacao_orgao_documento,
    SAFE_CAST(ordenacao_doc AS STRING) AS ordenacao_documento,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.documento`
WHERE SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

{% endif %}