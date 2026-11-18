{{ config(
    alias="mestre_ordem_servico",
    materialized="table",
    partition_by={"field": "criado_em", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["status", "servico"],
    on_schema_change="sync_all_columns",
) }}
{{ mart_czrm__mestre_ordem_servico("raw_czrm_staging__") }}
