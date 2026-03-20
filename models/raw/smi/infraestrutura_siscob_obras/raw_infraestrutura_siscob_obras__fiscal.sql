{{
    config(
        alias="fiscal",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

select 
    DISTINCT
        {{ clean_and_cast('cd_obra', 'string') }} id_obra,
        SAFE_CAST(
            SAFE.PARSE_DATE ('%Y-%m-%d', dt_inicio_vig) AS DATE
        ) AS data_inicio_vigencia,
        SAFE_CAST(
            SAFE.PARSE_DATE ('%Y-%m-%d', dt_fim_vig) AS DATE
        ) AS data_fim_vigencia,
        SAFE_CAST(matricula AS STRING) matricula,
        SAFE_CAST(nome AS STRING) nome,
        SAFE_CAST(email AS STRING) email
from {{ source('brutos_siscob_staging', 'fiscal') }} AS t