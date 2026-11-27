{{
    config(
        alias='afastamento_antigo',
        materialized="table",
        tags=["raw", "ergon", "afastamento"],
        description="Tabela que armazena as ocorrências de afastamento do servidor municipal no sistema antigo."
    )
}}

SELECT
    SAFE_CAST (REGEXP_REPLACE(TRIM(m10), r'\.0$', '') AS string) AS id_matricula_vinculo,
    DATETIME(sa_dt_afas_y2) AS data_inicio,
    DATETIME(sa_dt_prer_y2) AS data_previsao_retorno,
    DATETIME(sa_dt_retr_y2) AS data_fim
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_simpa_sirhu_afastamento_gbp') }} AS t