{{
    config(
      schema="brutos_taxirio",
      alias="descontos",
      materialized="table",
      partition_by={
        "field": "data_criacao",
        "data_type": "date",
        "granularity": "day"
      },
      tags=["raw", "taxirio"],
      description="Tabela de Descontos"
    )
}}
SELECT
  SAFE_CAST (id as STRING) as id_desconto_associado,
  SAFE_CAST (description as STRING) as descricao,
  SAFE_CAST (value as FLOAT64) as desconto,
  SAFE_CAST (createdAt as DATE) as data_criacao

FROM {{ source('brutos_taxirio_staging', 'discounts') }}
