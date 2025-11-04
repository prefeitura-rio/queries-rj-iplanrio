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
    SELECT 
    _airbyte_extracted_at,
    CAST(tipo AS STRING) as tipo,
    CAST(status AS STRING) as status,
    CAST(id_tipo AS STRING) as id_tipo,	
    CAST(subtipo AS STRING) as subtipo,
    CAST(data_fim AS STRING) as data_fim,
    CAST(latitude AS STRING) as latitude,	
    CAST(situacao AS STRING) as situacao,
    CAST(categoria AS STRING) as categoria,
    CAST(descricao AS STRING) as descricao,
    CAST(id_bairro AS STRING) as id_bairro,	
    CAST(longitude AS STRING) as longitude,	
    CAST(id_chamado AS STRING) as id_chamado,	
    CAST(id_subtipo AS STRING) as id_subtipo,	
    CAST(prazo_tipo AS STRING) as prazo_tipo,
    CAST(data_inicio AS STRING) as data_inicio,
    CAST(reclamacoes AS STRING) as reclamacoes,	
    CAST(tempo_prazo AS STRING) as tempo_prazo,	
    CAST(dentro_prazo AS STRING) as dentro_prazo,
    CAST(id_logradouro AS STRING) as id_logradouro,	
    CAST(prazo_unidade AS STRING) as prazo_unidade,
    CAST(tipo_situacao AS STRING)  as tipo_situacao,
    CAST(dt_atualizacao AS STRING) as dt_atualizacao,
    CAST(numero_logradouro AS STRING) as numero_logradouro,
    CAST(id_territorialidade AS STRING) as id_territorialidade,	
    CAST(id_origem_ocorrencia AS STRING) as id_origem_ocorrencia,	
    CAST(justificativa_status AS STRING) as justificativa_status,
    CAST(data_alvo_diagnostico AS STRING) as data_alvo_diagnostico,
    CAST(data_alvo_finalizacao AS STRING) as data_alvo_finalizacao,
    CAST(data_real_diagnostico AS STRING) as data_real_diagnostico,
    CAST(id_unidade_organizacional AS STRING) as id_unidade_organizacional,	
    CAST(nome_unidade_organizacional AS STRING) as nome_unidade_organizacional,
    CAST(id_unidade_organizacional_mae AS STRING) as id_unidade_organizacional_mae,	
    CAST(unidade_organizacional_ouvidoria AS STRING) as unidade_organizacional_ouvidoria
    FROM (
        -- Alguns ds_endereco_numero vem no formato "15,17,19" e selecionamos o primeiro que aparece
        SELECT
            * EXCEPT(numero_logradouro),
            CASE WHEN REGEXP_CONTAINS(CAST(numero_logradouro AS STRING), ",") THEN SPLIT(CAST(numero_logradouro AS STRING), ',')[SAFE_OFFSET(0)] ELSE numero_logradouro END AS numero_logradouro,
            row_number() OVER (PARTITION BY id_chamado ORDER BY data_fim DESC, data_inicio DESC) AS ranking
        FROM {{ source('brutos_1746_staging_airbyte', 'ViewDatalake') }} AS t
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
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.data_inicio) AS DATETIME
    ) AS data_inicio,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.data_fim) AS DATETIME
    ) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(ch.id_bairro, r'\.0$', '') AS STRING) id_bairro,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_territorialidade, r'\.0$', '') AS STRING
    ) id_territorialidade,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_logradouro, r'\.0$', '') AS STRING
    ) id_logradouro,
    SAFE_CAST(
      SAFE_CAST(ch.numero_logradouro AS FLOAT64) AS INT64
    ) numero_logradouro,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_unidade_organizacional, r'\.0$', '') AS STRING
    ) id_unidade_organizacional,
    SAFE_CAST(
        REGEXP_REPLACE(ch.nome_unidade_organizacional, r'\.0$', '') AS STRING
    ) nome_unidade_organizacional,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_unidade_organizacional_mae, r'\.0$', '') AS STRING
    ) id_unidade_organizacional_mae,
    SAFE_CAST(
        REGEXP_REPLACE(ch.unidade_organizacional_ouvidoria, r'\.0$', '') AS STRING
    ) unidade_organizacional_ouvidoria,
    SAFE_CAST(ch.categoria AS STRING) categoria,
    SAFE_CAST(REGEXP_REPLACE(ch.id_tipo, r'\.0$', '') AS STRING) id_tipo,
    SAFE_CAST(ch.tipo AS STRING) tipo,
    SAFE_CAST(
        REGEXP_REPLACE(ch.id_subtipo, r'\.0$', '') AS STRING
    ) id_subtipo,
    SAFE_CAST(ch.subtipo AS STRING) subtipo,
    SAFE_CAST(ch.status AS STRING) status,
    SAFE_CAST(geo.longitude AS FLOAT64) longitude,
    SAFE_CAST(geo.latitude AS FLOAT64) latitude,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.data_alvo_finalizacao) AS DATETIME
    ) AS data_alvo_finalizacao,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.data_alvo_diagnostico) AS DATETIME
    ) AS data_alvo_diagnostico,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ch.data_real_diagnostico) AS DATETIME
    ) AS data_real_diagnostico,
    SAFE_CAST(ch.tempo_prazo AS INT64) tempo_prazo,
    SAFE_CAST(
        REGEXP_REPLACE(ch.prazo_unidade, r'\.0$', '') AS STRING
    ) prazo_unidade,
    SAFE_CAST(ch.prazo_tipo AS STRING) prazo_tipo,
    SAFE_CAST(ch.dentro_prazo AS STRING) dentro_prazo,
    SAFE_CAST(ch.situacao AS STRING) situacao,
    SAFE_CAST(ch.tipo_situacao AS STRING) tipo_situacao,
    SAFE_CAST(ch.justificativa_status AS STRING) justificativa_status,
    SAFE_CAST(ch.reclamacoes AS INT64) reclamacoes,
    SAFE_CAST(ch.descricao AS STRING) descricao,
    _airbyte_extracted_at as extracted_at,
    dt_atualizacao as updated_at,
    SAFE_CAST(DATE_TRUNC(DATE(ch.data_inicio), month) AS DATE) data_particao,
    FROM chamados ch
    LEFT JOIN enderecos_geolocalizados geo
        ON SAFE_CAST(ch.id_logradouro AS FLOAT64) = CAST(geo.id_logradouro AS FLOAT64)
        AND SAFE_CAST(ch.numero_logradouro AS FLOAT64) = cast(geo.numero_porta as FLOAT64)