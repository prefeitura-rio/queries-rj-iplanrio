{% macro mart_czrm__mestre_andamento(ref_prefix) %}

select
    a.id                                            as andamento_id,
    a.work_order                                    as os_id,
    os.work_order_number                            as numero_os,
    c.case_number                                   as numero_protocolo,
    coalesce(a.data_hora_andamento, a.created_date) as data_hora,
    a.data_hora_andamento                           as informado_em,
    a.event_type                                    as tipo,
    a.status_normalizado                            as status,
    a.status                                        as situacao,
    a.name                                          as titulo,
    a.modified_comments                             as descricao,
    u.name                                          as criado_por,
    a.is_visible_to_citizen                         as visivel_cidadao,
    a.publication_datetime                          as publicado_em

from {{ ref(ref_prefix ~ 'work_order_update') }} a
left join (
    select id, work_order_number, case_id
    from {{ ref(ref_prefix ~ 'work_order') }}
) os on a.work_order = os.id
left join (
    select id, case_number
    from {{ ref(ref_prefix ~ 'service_case') }}
) c on os.case_id = c.id
left join (
    select id, name
    from {{ ref(ref_prefix ~ 'user') }}
) u on a.created_by_id = u.id
where not coalesce(a.is_deleted, false)

{% endmacro %}
