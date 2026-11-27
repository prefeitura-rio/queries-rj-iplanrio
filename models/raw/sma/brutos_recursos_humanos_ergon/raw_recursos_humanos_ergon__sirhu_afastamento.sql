{{
    config(
        alias='sirhu_afastamento',
        materialized="table",
        tags=["raw", "ergon", "afastamento", "sirhu"],
        description="Afastamentos do sistema SIRHU."
    )
}}

SELECT
    SAFE_CAST(emp_codigo AS STRING) AS id_empresa,
    SAFE_CAST(matricula AS STRING) AS id_matricula,
    SAFE_CAST(afast_cod AS STRING) AS id_afastamento,
    SAFE_CAST(dt_afas_y2 AS DATETIME) AS data_inicio,
    SAFE_CAST(dt_prer_y2 AS DATETIME) AS data_previsao_retorno,
    SAFE_CAST(dt_retr_y2 AS DATETIME) AS data_fim,
    SAFE_CAST(num_ato_of AS STRING) AS numero_ato_oficial,
    SAFE_CAST(id_mov AS STRING) AS id_movimentacao,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'sirhu_afastamento') }} AS t
