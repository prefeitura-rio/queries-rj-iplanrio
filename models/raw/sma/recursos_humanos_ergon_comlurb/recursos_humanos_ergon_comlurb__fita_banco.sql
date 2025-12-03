{{
    config(
        alias='fita_banco',
        schema='recursos_humanos_ergon_comlurb',
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}
SELECT
    SAFE_CAST(DATE(dtexerc) AS DATE) AS data_inicio_exercicio,
    SAFE_CAST(DATE(dtaposent) AS DATE) AS data_aposentadoria,
    SAFE_CAST(DATE(dtvac) AS DATE) AS data_vacancia,
    SAFE_CAST(REGEXP_REPLACE(TRIM(setor), r'\.0$', '') AS STRING) AS id_setor,
    SAFE_CAST(REGEXP_REPLACE(valorvan, r',', '.') AS FLOAT64) AS valor,    SAFE_CAST(REGEXP_REPLACE(valordes, r',', '.') AS FLOAT64) AS valor_desconto,    SAFE_CAST(REGEXP_REPLACE(TRIM(numdepen), r'\.0$', '') AS INT64) AS numero_dependente,
    SAFE_CAST(TRIM(agencia) AS STRING) AS agencia,
    SAFE_CAST(TRIM(banco) AS STRING) AS banco,
    SAFE_CAST(TRIM(conta) AS STRING) AS conta,
    SAFE_CAST(REGEXP_REPLACE(valorliq, r',', '.') AS FLOAT64) AS valor_liquido,    SAFE_CAST(TRIM(cargo) AS STRING) AS cargo,
    SAFE_CAST(TRIM(referencia) AS STRING) AS referencia,
    SAFE_CAST(TRIM(funcao) AS STRING) AS funcao,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(ficha), r'\.0$', '') AS STRING) AS id_ficha,
    SAFE_CAST(TRIM(regimejur) AS STRING) AS regime_juridico,
    SAFE_CAST(TRIM(tipovinc) AS STRING) AS tipo_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(lancamento), r'\.0$', '') AS STRING) AS id_lancamento,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(DATE(mes_ano) AS DATE) AS data_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numero), r'\.0$', '') AS INT64) AS numero_folha,
    SAFE_CAST(TRIM(rubrica) AS STRING) AS rubrica,
    SAFE_CAST(TRIM(categoria) AS STRING) AS categoria_vinculo,
    SAFE_CAST(TRIM(data_credito) AS STRING) AS data_credito,
    SAFE_CAST(REGEXP_REPLACE(TRIM(jornada), r'\.0$', '') AS STRING) AS id_jornada,
    SAFE_CAST(TRIM(subcategoria) AS STRING) AS subcategoria_cargo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cpf), r'\.0$', '') AS STRING) AS id_cpf,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM rj-smfp.recursos_humanos_ergon_comlurb_staging.fita_banco AS t
WHERE
    SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

{% endif %}