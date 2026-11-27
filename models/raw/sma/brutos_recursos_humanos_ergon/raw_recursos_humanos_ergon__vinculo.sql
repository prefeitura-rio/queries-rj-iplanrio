{{
    config(
        alias='vinculo',
        materialized="table",
        tags=["raw", "ergon", "vinculo"],
        description="Tabela que contém os registros dos vínculos funcionais da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    safe_cast (NUMFUNC AS string) AS id_funcionario,
    safe_cast (NUMERO AS string) AS id_vinculo,
    safe_cast (TIPOVINC AS STRING) AS tipo_vinculo,
    safe_cast (MATRIC AS STRING) AS id_matricula_vinculo,
    DATETIME(DTNOM) AS data_nomeacao,
    DATETIME(DTPOSSE) AS data_posse,
    DATETIME(DTEXERC) AS data_exercicio,
    safe_cast (REGIMEJUR AS STRING) AS regime_juridico,
    safe_cast (CATEGORIA AS STRING) AS categoria,
    safe_cast (DESCONTA_IR AS STRING) AS desconta_ir,
    safe_cast (MOTIVO AS STRING) AS descricao_provimento,
    safe_cast (
        REGEXP_REPLACE(CAST(CLASSIFCONC AS STRING), r'\.0$', '') AS STRING
    ) AS classificacao_concurso,
    DATETIME(DTINI_CESSAO) AS data_inicio_cessao,
    DATETIME(DTFIM_CESSAO) AS data_fim_cessao,
    DATETIME(DTCONC) AS data_concurso,
    DATETIME(DTOPFGTS) AS data_fgts,
    DATETIME(DTINICONTR) AS data_inicio_contrato,
    DATETIME(DTFIMCONTR) AS data_fim_contrato,
    DATETIME(DTPRORROGCONTR) AS data_prorrogacao_contrato,
    DATETIME(DTAPOSENT) AS data_aposentadoria,
    safe_cast (TIPOAPOS AS STRING) AS tipo_aposentadoria,
    DATETIME(DTVAC) AS data_vacancia,
    safe_cast (FORMAVAC AS string) AS id_forma_vacancia,
    safe_cast (MOTIVOVAC AS STRING) AS motivo_vacancia,
    safe_cast (NUMVINCANT AS string) AS id_vinculo_anterior,
    safe_cast (NUMVINCPOS AS string) AS id_vinculo_posterior,
    safe_cast (TIPOORG_REQ AS STRING) AS tipo_orgao_origem,
    safe_cast (FUNCAO_REQ AS STRING) AS funcao_origem_requisicao,
    safe_cast (FONE_REQ AS STRING) AS telefone_requisicao,
    safe_cast (ORGAO_EXT_REQ AS STRING) AS orgao_origem_requisicao,
    safe_cast (TIPO_ONUS_REQ AS STRING) AS tipo_onus_requisicao,
    safe_cast (TIPO_RESSARC_REQ AS STRING) AS tipo_ressarcimento_requisicao,
    safe_cast (TIPO_REQ AS STRING) AS tipo_requisicao,
    DATETIME(DT_PGTO_ATE) AS data_pagamento_requisicao,
    safe_cast (FLEX_CAMPO_01 AS STRING) AS origem_servidor_cedido,
    safe_cast (FLEX_CAMPO_05 AS STRING) AS id_processo_origem,
    safe_cast (FLEX_CAMPO_09 AS STRING) AS contrato_suspenso,
    safe_cast (FLEX_CAMPO_18 AS STRING) AS cota,
    safe_cast (EMP_CODIGO AS string) AS id_empresa,
    DATETIME(DT_HOMOLOG) AS data_homologacao_aposentadoria,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_vinculos') }}