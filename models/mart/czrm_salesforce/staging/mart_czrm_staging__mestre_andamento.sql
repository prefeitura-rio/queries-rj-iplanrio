{{ config(
    alias="mestre_andamento",
    materialized="table",
    partition_by={"field": "data_hora", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["tipo", "status"],
    on_schema_change="sync_all_columns",
) }}
{{ mart_czrm__mestre_andamento("raw_czrm_staging__") }}
