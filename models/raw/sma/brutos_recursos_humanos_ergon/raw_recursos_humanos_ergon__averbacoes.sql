{{
    config(
        alias='averbacoes',
        materialized="table",
        tags=["raw", "ergon", "averbacoes"],
        description="Tabela que armazena o cadastro das averbações."
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS int64) AS id_funcionario,
    SAFE_CAST(NUMVINC AS int64) AS id_vinculo,
    SAFE_CAST(CHAVE AS int64) AS id_averbacao,
    safe_cast(SAFE_CAST(DTINI AS timestamp) as date) AS data_inicio,
    safe_cast(SAFE_CAST(DTFIM AS timestamp) as date) AS data_final,
    SAFE_CAST(INSTITUICAO AS STRING) AS instituicao,
    SAFE_CAST(TIPOTEMPO AS STRING) AS id_tipo_tempo,
    safe_cast(SAFE_CAST(DATA_A_CONTAR AS timestamp) as date) AS data_validade,
    SAFE_CAST(TOTAL_DIAS AS int64) AS total_dias_averbados,
    SAFE_CAST(MOTIVO AS STRING) AS motivo,
    case SAFE_CAST(SOBREPOE AS STRING)
      when 'S' then true
      else false
    end as sobrepoe,
    SAFE_CAST(EMP_CODIGO AS int64) AS id_empresa,
    SAFE_CAST(OBS AS STRING) AS obs,
    SAFE_CAST(REGPREV AS STRING) AS regime_previdenciario
FROM {{ source('brutos_ergon_staging', 'AVERBACOES_CONTA') }} AS t