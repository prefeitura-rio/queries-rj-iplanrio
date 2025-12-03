{{
    config(
        materialized='table',
        alias='fita_banco',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}
SELECT
    SAFE_CAST(REGEXP_REPLACE(lancamento, r'\.0$', '') AS STRING) AS lancamento,
    SAFE_CAST(mes_ano AS DATE) AS mes_ano,
    SAFE_CAST(dtexerc AS DATE) AS dtexerc,
    SAFE_CAST(dtaposent AS DATE) AS dtaposent,
    SAFE_CAST(dtvac AS DATE) AS dtvac,
    SAFE_CAST(data_credito AS DATE) AS data_credito,
    SAFE_CAST(REGEXP_REPLACE(numfunc, r'\.0$', '') AS STRING) AS numfunc,
    SAFE_CAST(REGEXP_REPLACE(numvinc, r'\.0$', '') AS STRING) AS numvinc,
    SAFE_CAST(REGEXP_REPLACE(numero, r'\.0$', '') AS INT64) AS numero,
    SAFE_CAST(rubrica AS STRING) AS rubrica,
    SAFE_CAST(setor AS STRING) AS setor,
    SAFE_CAST(REGEXP_REPLACE(valorvan, r',', '.') AS FLOAT64) AS valorvan,
    SAFE_CAST(REGEXP_REPLACE(valordes, r',', '.') AS FLOAT64) AS valordes,
    SAFE_CAST(REGEXP_REPLACE(valorliq, r'\.0$', '') AS FLOAT64) AS valorliq,
    SAFE_CAST(REGEXP_REPLACE(numpens, r'\.0$', '') AS INT64) AS numpens,
    SAFE_CAST(REGEXP_REPLACE(numdepen, r'\.0$', '') AS INT64) AS numdepen,
    SAFE_CAST(REGEXP_REPLACE(agencia, r'\.0$', '') AS INT64) AS agencia,
    SAFE_CAST(REGEXP_REPLACE(banco, r'\.0$', '') AS INT64) AS banco,
    SAFE_CAST(REGEXP_REPLACE(conta, r'\.0$', '') AS INT64) AS conta_banco,
    SAFE_CAST(REGEXP_REPLACE(cargo, r'\.0$', '') AS STRING) AS cargo,
    SAFE_CAST(subcategoria AS STRING) AS subcategoria,
    SAFE_CAST(REGEXP_REPLACE(referencia, r'\.0$', '') AS STRING) AS referencia,
    SAFE_CAST(funcao AS STRING) AS funcao,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(emp_codigo, r'\.0$', '') AS STRING) AS emp_codigo,
    SAFE_CAST(REGEXP_REPLACE(ficha, r'\.0$', '') AS STRING) AS ficha,
    SAFE_CAST(REGEXP_REPLACE(regimejur, r'\.0$', '') AS STRING) AS regimejur,
    SAFE_CAST(tipovinc AS STRING) AS tipovinc,
    SAFE_CAST(categoria AS STRING) AS categoria,
    SAFE_CAST(cpf AS STRING) AS cpf,
    SAFE_CAST(REGEXP_REPLACE(flex_campo_05, r'\.0$', '') AS STRING) AS flex_campo_05,
    SAFE_CAST(REGEXP_REPLACE(jornada, r'\.0$', '') AS STRING) AS jornada,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM {{ source('brutos_ergon_saude_staging', 'VW_DLK_ERG_FITA_BANCO') }} AS t
-- WHERE
--     SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')

-- {% if is_incremental() %}

-- {% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

-- AND
--     SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

-- {% endif %}