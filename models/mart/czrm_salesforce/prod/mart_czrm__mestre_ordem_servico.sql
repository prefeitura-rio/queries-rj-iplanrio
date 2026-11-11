{{ config(
    alias="mestre_ordem_servico",
    materialized="table",
    partition_by={"field": "criado_em", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["status", "servico"],
    tags=["quarter_hourly", "czrm"],
) }}

with

os as (
    select * from {{ ref('raw_czrm__work_order') }}
),

protocolo as (
    select * from {{ ref('raw_czrm__service_case') }}
),

servico as (
    select * from {{ ref('raw_czrm__service_catalog') }}
),

uo as (
    select * from {{ ref('raw_czrm__organizational_unity') }}
),

usuario as (
    select * from {{ ref('raw_czrm__user') }}
),

fila as (
    select * from {{ ref('raw_czrm__queue_group') }}
),

bairro as (
    select * from {{ ref('raw_czrm__neighborhood') }}
),

conta as (
    select * from {{ ref('raw_czrm__account') }}
),

andamentos as (
    select
        work_order as os_id,
        count(*) as qtd_andamentos,
        array_agg(
            struct(
                name                as titulo,
                event_type          as tipo,
                status_normalizado  as status,
                data_hora_andamento as data_hora,
                publication_datetime as publicado_em
            )
            order by data_hora_andamento desc
            limit 1
        )[offset(0)] as ultimo
    from {{ ref('raw_czrm__work_order_update') }}
    group by work_order
),

respostas as (
    select
        work_order as os_id,
        count(*) as qtd_respostas,
        countif(is_internal) as respostas_internas,
        to_json_string(
            array_agg(struct(attribute_label as campo, value_display as valor))
        ) as respostas_json
    from {{ ref('raw_czrm__work_order_attribute_answer') }}
    group by work_order
)

select
    -- chaves
    cast(os.id as string)                        as os_id,
    cast(os.work_order_number as string)         as numero_os,
    cast(p.case_number as string)                as numero_protocolo,
    cast(os.created_date as timestamp)           as criado_em,
    cast(os.system_modstamp as timestamp)        as atualizado_em,

    -- status e classificação
    cast(os.status as string)                    as status,
    cast(os.is_wo_closed as boolean)             as encerrada,
    cast(os.priority as string)                  as prioridade,
    cast(os.canal_atendimento as string)         as canal_atendimento,
    cast(os.tipo_solicitante as string)          as tipo_solicitante,
    cast(os.close_reason as string)              as motivo_encerramento,
    cast(os.categoria as string)                 as tipo_categoria_servico,
    cast(os.description as string)               as descricao_wo,

    -- serviço, UO e responsáveis
    cast(s.service_name as string)               as servico,
    cast(s.theme as string)                      as servico_tema,
    cast(s.subtheme as string)                   as servico_subtema,
    cast(uo.shortname as string)                 as uo_sigla,
    cast(uo.name as string)                      as uo_nome,
    cast(os.main_organizational_unit as string)  as orgao_central,
    cast(coalesce(u.name, f.name) as string)     as dono_nome,
    cast(f.name as string)                       as fila,
    cast(criador.name as string)                 as criado_por,

    -- datas
    cast(os.data_fechamento as timestamp)        as fechada_em,
    cast(os.sla_target_date as timestamp)        as sla_prazo,

    -- local
    cast(os.public_way as string)                as logradouro,
    cast(os.door_number as string)               as numero,
    cast(os.no_number as boolean)                as sem_numero,
    cast(os.complement as string)                as complemento,
    cast(os.landmark as string)                  as referencia,
    cast(b.name as string)                       as bairro,
    cast(b.subprefeitura as string)              as subprefeitura,
    cast(b.ra as string)                         as regiao_administrativa,
    cast(os.postal_code as string)               as cep,
    cast(os.city as string)                      as cidade,
    cast(os.state as string)                     as uf,
    cast(os.latitude as float64)                 as latitude,
    cast(os.longitude as float64)                as longitude,
    cast(os.geolocation_precise as boolean)      as precisao_geo,
    cast(os.favela_name as string)               as favela,
    cast(os.public_way_code as string)           as codigo_logradouro,

    -- solicitante
    cast(os.solicitante_nome as string)          as solicitante_nome,
    cast(os.solicitante_telefone as string)      as solicitante_telefone,
    cast(c.nome_completo as string)              as nome_completo,
    cast(c.cpf as string)                        as cpf,
    cast(c.person_email as string)               as email,
    cast(c.phone_1 as string)                    as telefone_1,
    cast(c.phone_1_type as string)               as telefone_1_tipo,
    cast(c.phone_2 as string)                    as telefone_2,
    cast(c.phone_3 as string)                    as telefone_3,

    -- protocolo
    cast(p.status as string)                     as protocolo_status,
    cast(p.subject as string)                    as protocolo_assunto,
    cast(p.origin as string)                     as protocolo_origem,
    cast(p.priority as string)                   as protocolo_prioridade,
    cast(p.created_date as timestamp)            as protocolo_criado_em,
    cast(p.closed_date as timestamp)             as protocolo_fechado_em,
    cast(p.is_closed as boolean)                 as protocolo_fechado,

    -- último andamento
    cast(coalesce(a.qtd_andamentos, 0) as int64)         as qtd_andamentos,
    cast(a.ultimo.data_hora as timestamp)                 as ultimo_andamento_em,
    cast(a.ultimo.titulo as string)                       as ultimo_andamento,
    cast(a.ultimo.tipo as string)                         as ultimo_andamento_tipo,
    cast(a.ultimo.status as string)                       as ultimo_andamento_status,
    cast(a.ultimo.publicado_em as timestamp)              as ultimo_andamento_publicado_em,

    -- vínculos
    cast(pai.work_order_number as string)                 as os_pai_numero,
    cast(os.equivalent_count as int64)                    as qtd_equivalentes,
    cast(os.is_equivalencia as boolean)                   as e_equivalencia,

    -- formulário
    cast(coalesce(r.qtd_respostas, 0) as int64)           as qtd_respostas,
    parse_json(coalesce(r.respostas_json, '[]'))           as respostas_json,
    cast(coalesce(r.respostas_internas, 0) as int64)      as respostas_internas

from os
left join protocolo p        on os.case_id             = p.id
left join servico s          on os.service             = s.id
left join uo                 on os.assigned_uo         = uo.id
left join usuario u          on os.owner_id            = u.id
left join fila f             on os.owner_id            = f.id
left join usuario criador    on os.created_by_id       = criador.id
left join bairro b           on os.neighborhood        = b.id
left join conta c            on os.account_id          = c.id
left join andamentos a       on os.id                  = a.os_id
left join respostas r        on os.id                  = r.os_id
left join os pai             on os.parent_work_order_id = pai.id
