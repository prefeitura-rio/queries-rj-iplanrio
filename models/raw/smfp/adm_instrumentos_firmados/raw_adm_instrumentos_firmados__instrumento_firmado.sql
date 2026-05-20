{{config(
    materialized = 'table',
    alias = 'instrumento_firmado'
)
}}

SELECT
    SAFE_CAST(exercicio AS STRING) ano_exercicio,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numero), r'\.0$', '') AS STRING) AS id_contrato,
    SAFE_CAST(SAFE.PARSE_DATE("%d/%m/%Y",data_inicio_prev) AS DATE) AS data_inicio_prevista,
    SAFE_CAST(SAFE.PARSE_DATE("%d/%m/%Y",data_fim_prev) AS DATE) AS data_fim_prevista,
    SAFE_CAST(SAFE.PARSE_DATE("%d/%m/%Y",data_assinatura) AS DATE) AS data_assinatura,
    SAFE_CAST(favorecido AS STRING) nome_favorecido,
    SAFE_CAST(cnpj AS STRING) cnpj_cpf_favorecido,
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(tipo_favorecido, r'^1$') THEN 'Pessoa Física'
            WHEN REGEXP_CONTAINS(tipo_favorecido, r'^2$') THEN 'Pessoa Jurídica'
            ELSE tipo_favorecido
        END AS STRING
    ) AS tipo_favorecido,
    SAFE_CAST(REGEXP_REPLACE(TRIM(orgao_executor), r'\.0$', '') AS STRING) AS id_orgao_executor,
    SAFE_CAST(descricao_orgao_executor AS STRING) nome_orgao_executor,
    SAFE_CAST(objeto AS STRING) objeto,
    SAFE_CAST(especie AS STRING) tipo_instrumento ,
    SAFE_CAST(situacao AS STRING) status_instrumento,
    SAFE_CAST(REGEXP_REPLACE(TRIM(processo), r'\.0$', '') AS STRING) AS id_processo_instrutivo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(pt), r'\.0$', '') AS STRING) AS id_programa_trabalho,
    SAFE_CAST(modalidade_licitacao AS STRING) modalidade_licitacao,
    SAFE_CAST(embasamento_legal AS STRING) embasamento_legal,
    SAFE_CAST(natureza AS STRING) natureza_despesa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(fonte), r'\.0$', '') AS STRING) AS id_fonte_recurso,
    SAFE_CAST(valor_pago AS FLOAT64) valor_pago,
    SAFE_CAST(saldo_exec AS FLOAT64) valor_saldo_executado,
    SAFE_CAST(valor_empenhado AS FLOAT64) valor_empenhado,
    SAFE_CAST(valor_liquidado AS FLOAT64) valor_liquidado,
    SAFE_CAST(valor_atualizado AS FLOAT64) valor_atualizado
    FROM {{ source('adm_instrumentos_firmados_staging', 'instrumento_firmado') }}
