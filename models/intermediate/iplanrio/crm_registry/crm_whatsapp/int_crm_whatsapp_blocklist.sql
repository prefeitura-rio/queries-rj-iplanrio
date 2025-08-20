{{ config(
    materialized = 'table',
    alias="blocklist",
    schema="brutos_wetalkie",
    tags=["hourly"],
    partition_by={
        "field": "data_particao",
        "data_type": "date"
    }
) }}

SELECT *
FROM {{ ref('raw_iplanrio_wetalkie__blocklist') }}
