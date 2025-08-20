{{
    config(
        alias='chamado',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_chamado',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

WITH chamados AS (
    -- Elimina linhas duplicadas
    SELECT * FROM (
        -- Alguns ds_endereco_numero vem no formato "15,17,19" e selecionamos o primeiro que aparece
        SELECT
            * EXCEPT(ds_endereco_numero),
            CASE WHEN REGEXP_CONTAINS(CAST(ds_endereco_numero AS STRING), ",") THEN SPLIT(CAST(ds_endereco_numero AS STRING), ',')[SAFE_OFFSET(0)] ELSE ds_endereco_numero END AS ds_endereco_numero,
            row_number() OVER (PARTITION BY id_chamado ORDER BY dt_fim DESC, data_particao DESC) AS ranking
        FROM {{ source('brutos_1746_staging', 'chamado') }} AS t

        -- {% if is_incremental() %}

        -- -- this filter will only be applied on an incremental run
        -- WHERE DATE(data_particao) > (SELECT max(DATE(data_particao)) FROM {{ this }})

        -- {% endif %}
    )
    WHERE ranking=1
),

enderecos_geolocalizados AS (
    -- Elimina linhas duplicadas
    SELECT * FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY id_logradouro, SAFE_CAST(SAFE_CAST(numero_porta AS FLOAT64) AS STRING) ) AS ranking
        FROM `rj-escritorio-dev.dados_mestres.enderecos_geolocalizados`
        WHERE numero_porta IS NOT NULL
    )
    WHERE ranking=1
)

SELECT DISTINCT
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_chamado, r'\.0$', '') AS STRING
    ) id_chamado,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_origem_ocorrencia, r'\.0$', '') AS STRING
    ) id_origem_ocorrencia,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.dt_inicio) AS DATETIME
    ) AS data_inicio,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.dt_fim) AS DATETIME
    ) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(ch.id_bairro, r'\.0$', '') AS STRING) id_bairro,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_territorialidade, r'\.0$', '') AS STRING
    ) id_territorialidade,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_logradouro, r'\.0$', '') AS STRING
    ) id_logradouro,
    SAFE_CAST(
      SAFE_CAST(ch.ds_endereco_numero AS FLOAT64) AS INT64
    ) numero_logradouro,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_unidade_organizacional, r'\.0$', '') AS STRING
    ) id_unidade_organizacional,
    SAFE_CAST(
        REGEXP_REPLACE(ch.no_unidade_organizacional, r'\.0$', '') AS STRING
    ) nome_unidade_organizacional,
    SAFE_CAST(
        REGEXP_REPLACE(ch.uo_mae, r'\.0$', '') AS STRING
    ) id_unidade_organizacional_mae,
    SAFE_CAST(
        REGEXP_REPLACE(ch.fl_ouvidoria, r'\.0$', '') AS STRING
    ) unidade_organizacional_ouvidoria,
    SAFE_CAST(ch.no_categoria AS STRING) categoria,
    SAFE_CAST(REGEXP_REPLACE(ch.id_tipo, r'\.0$', '') AS STRING) id_tipo,
    SAFE_CAST(ch.no_tipo AS STRING) tipo,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_subtipo, r'\.0$', '') AS STRING
    ) id_subtipo,
    SAFE_CAST(ch.no_subtipo AS STRING) subtipo,
    SAFE_CAST(ch.no_status AS STRING) status,
    SAFE_CAST(geo.longitude AS FLOAT64) longitude,
    SAFE_CAST(geo.latitude AS FLOAT64) latitude,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.dt_alvo_finalizacao) AS DATETIME
    ) AS data_alvo_finalizacao,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.dt_alvo_diagnostico) AS DATETIME
    ) AS data_alvo_diagnostico,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.dt_real_diagnostico) AS DATETIME
    ) AS data_real_diagnostico,
    SAFE_CAST(ch.nu_prazo AS INT64) tempo_prazo,
    SAFE_CAST(
        REGEXP_REPLACE(ch.ic_prazo_unidade_tempo, r'\.0$', '') AS STRING
    ) prazo_unidade,
    SAFE_CAST(ch.ic_prazo_tipo AS STRING) prazo_tipo,
    SAFE_CAST(ch.prazo AS STRING) dentro_prazo,
    SAFE_CAST(ch.situacao AS STRING) situacao,
    SAFE_CAST(ch.tipo_situacao AS STRING) tipo_situacao,
    SAFE_CAST(ch.no_justificativa AS STRING) justificativa_status,
    SAFE_CAST(ch.reclamacoes AS INT64) reclamacoes,
    SAFE_CAST(ch.ds_chamado AS STRING) descricao,
    SAFE_CAST(DATE_TRUNC(DATE(ch.data_particao), month) AS DATE) data_particao,
    FROM chamados ch
    LEFT JOIN enderecos_geolocalizados geo
        ON SAFE_CAST(ch.id_logradouro AS FLOAT64) = CAST(geo.id_logradouro AS FLOAT64)
        AND SAFE_CAST(ch.ds_endereco_numero AS FLOAT64) = cast(geo.numero_porta as FLOAT64)