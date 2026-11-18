{{
    config(
        alias="work_order_relationship",
        materialized="incremental",
        unique_key="id",
        partition_by={
            "field": "system_modstamp",
            "data_type": "datetime",
            "granularity": "day",
        },
        on_schema_change="sync_all_columns",
    )
}}
{{ raw_czrm__work_order_relationship("czrm_salesforce_org_producao") }}
