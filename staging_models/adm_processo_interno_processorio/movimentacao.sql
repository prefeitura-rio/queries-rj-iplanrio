{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


SELECT
    SAFE_CAST(dt_mov AS DATE) AS data_movimentacao,
    SAFE_CAST(dt_fim_mov AS DATE) AS data_fim_movimentacao,
    SAFE_CAST(REGEXP_REPLACE(id_mov, r'\.0$', '') AS	STRING) AS 	id_movimentacao,
    SAFE_CAST(REGEXP_REPLACE(id_tp_mov, r'\.0$', '') AS	STRING) AS 	id_tipo_movimentacao,
    SAFE_CAST(REGEXP_REPLACE(id_cadastrante, r'\.0$', '')	AS	STRING) AS id_cadastrante,
    SAFE_CAST(REGEXP_REPLACE(id_lota_cadastrante, r'\.0$', '') AS STRING) AS id_lotacao_cadastrante,
    SAFE_CAST(REGEXP_REPLACE(id_mov_ref, r'\.0$', '') AS STRING) AS id_movimento_anterior,
    SAFE_CAST(REGEXP_REPLACE(id_mobil, r'\.0$', '') AS STRING) AS id_mobil,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.movimentacao`
WHERE SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')


{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

{% endif %}
