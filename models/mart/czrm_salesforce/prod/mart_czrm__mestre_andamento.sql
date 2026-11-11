{{ config(
    alias="mestre_andamento",
    materialized="table",
    partition_by={"field": "data_hora", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["tipo", "status"],
    tags=["quarter_hourly", "czrm"],
) }}

select
    cast(a.id as string)                    as andamento_id,
    cast(a.work_order as string)            as os_id,
    cast(os.work_order_number as string)    as numero_os,
    cast(c.case_number as string)           as numero_protocolo,
    cast(a.data_hora_andamento as timestamp) as data_hora,
    cast(a.event_type as string)            as tipo,
    cast(a.status_normalizado as string)    as status,
    cast(a.name as string)                  as titulo,
    cast(a.modified_comments as string)     as descricao,
    cast(u.name as string)                  as criado_por,
    cast(a.is_visible_to_citizen as boolean) as visivel_cidadao,
    cast(a.publication_datetime as timestamp) as publicado_em

from {{ ref('raw_czrm__work_order_update') }} a
left join {{ ref('raw_czrm__work_order') }} os on a.work_order = os.id
left join {{ ref('raw_czrm__service_case') }} c on os.case_id = c.id
left join {{ ref('raw_czrm__user') }} u on a.created_by_id = u.id
