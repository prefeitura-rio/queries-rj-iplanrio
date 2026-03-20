{{
    config(
        alias='afastamento_antigo',
        materialized="table",
        tags=["raw", "ergon", "afastamento"],
        description="Tabela que armazena as ocorrências de afastamento do servidor municipal no sistema antigo."
    )
}}

SELECT
    {{ clean_and_cast('m10', 'string', trim=true) }} AS id_matricula_vinculo,
    safe_cast(SAFE_CAST(TRIM(sa_dt_afas_y2) AS timestamp) as date) AS data_inicio,
    safe_cast(SAFE_CAST(TRIM(sa_dt_prer_y2) AS timestamp) as date) AS data_previsao_retorno,
    safe_cast(SAFE_CAST(TRIM(sa_dt_retr_y2) AS timestamp) as date) AS data_fim
FROM {{ source('brutos_ergon_staging', 'VW_SIMPA_SIRHU_AFASTAMENTO_GBP') }} AS t