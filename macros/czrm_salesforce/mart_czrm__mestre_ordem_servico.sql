{% macro mart_czrm__mestre_ordem_servico(ref_prefix) %}

with

os as (
    select
        id,
        work_order_number,
        case_id,
        created_date,
        system_modstamp,
        status,
        is_wo_closed,
        priority,
        canal_atendimento,
        tipo_solicitante,
        close_reason,
        categoria,
        description,
        main_organizational_unit,
        data_fechamento,
        sla_target_date,
        public_way,
        door_number,
        no_number,
        complement,
        landmark,
        neighborhood,
        postal_code,
        city,
        state,
        latitude,
        longitude,
        geolocation_precise,
        favela_name,
        public_way_code,
        solicitante_nome,
        solicitante_telefone,
        service,
        assigned_uo,
        owner_id,
        created_by_id,
        account_id,
        parent_work_order_id,
        equivalent_count,
        is_equivalencia
    from {{ ref(ref_prefix ~ 'work_order') }}
    where not coalesce(is_deleted, false)
),

protocolo as (
    select
        id,
        case_number,
        status,
        subject,
        origin,
        priority,
        created_date,
        closed_date,
        is_closed
    from {{ ref(ref_prefix ~ 'service_case') }}
    where not coalesce(is_deleted, false)
),

servico as (
    select
        id,
        service_name,
        theme_formula,
        subtheme_formula
    from {{ ref(ref_prefix ~ 'service_catalog') }}
),

uo as (
    select
        id,
        shortname,
        name
    from {{ ref(ref_prefix ~ 'organizational_unity') }}
),

usuario as (
    select
        id,
        name
    from {{ ref(ref_prefix ~ 'user') }}
),

fila as (
    select
        id,
        name
    from {{ ref(ref_prefix ~ 'queue_group') }}
),

bairro as (
    select
        id,
        name,
        subprefeitura,
        ra
    from {{ ref(ref_prefix ~ 'neighborhood') }}
),

conta as (
    select
        id,
        nome_completo,
        cpf,
        person_email,
        phone_1,
        phone_1_type,
        phone_2,
        phone_3
    from {{ ref(ref_prefix ~ 'account') }}
),

andamentos as (
    select
        work_order as os_id,
        count(*)   as qtd_andamentos
    from {{ ref(ref_prefix ~ 'work_order_update') }}
    where not coalesce(is_deleted, false)
    group by work_order
),

-- Último andamento: mesma regra do CRM (WorkOrderLastUpdateStatusService e
-- WorkOrdersSelector): CreatedDate DESC, Id DESC, excluindo registros em Rascunho.
-- O campo nativo WorkOrder.Last_Update_Status__c é calculado com essa mesma ordem.
ultimo_andamento as (
    select
        work_order                                  as os_id,
        name                                        as codigo,
        event_type                                  as tipo,
        status_normalizado                          as status_orgao,
        status                                      as situacao,
        coalesce(data_hora_andamento, created_date) as ocorrido_em,
        data_hora_andamento                         as informado_em,
        publication_datetime                        as publicado_em
    from {{ ref(ref_prefix ~ 'work_order_update') }}
    where not coalesce(is_deleted, false)
      and status <> 'Rascunho'
    qualify row_number() over (
        partition by work_order
        order by created_date desc, id desc
    ) = 1
),

respostas as (
    select
        work_order as os_id,
        count(*) as qtd_respostas,
        countif(is_internal) as respostas_internas,
        to_json_string(
            array_agg(struct(attribute_label as campo, value_display as valor))
        ) as respostas_json
    from {{ ref(ref_prefix ~ 'work_order_attribute_answer') }}
    where not coalesce(is_deleted, false)
    group by work_order
)

select
    -- chaves
    os.id                        as os_id,
    os.work_order_number         as numero_os,
    p.case_number                as numero_protocolo,
    os.created_date              as criado_em,
    os.system_modstamp           as atualizado_em,

    -- status e classificação
    os.status,
    os.is_wo_closed              as encerrada,
    os.priority                  as prioridade,
    os.canal_atendimento,
    os.tipo_solicitante,
    os.close_reason              as motivo_encerramento,
    os.categoria                 as tipo_categoria_servico,
    os.description               as descricao_wo,

    -- serviço, UO e responsáveis
    s.service_name               as servico,
    s.theme_formula              as servico_tema,
    s.subtheme_formula           as servico_subtema,
    uo.shortname                 as uo_sigla,
    uo.name                      as uo_nome,
    os.main_organizational_unit  as orgao_central,
    coalesce(u.name, f.name)     as dono_nome,
    f.name                       as fila,
    criador.name                 as criado_por,

    -- datas
    os.data_fechamento           as fechada_em,
    os.sla_target_date           as sla_prazo,

    -- local
    os.public_way                as logradouro,
    os.door_number               as numero,
    os.no_number                 as sem_numero,
    os.complement                as complemento,
    os.landmark                  as referencia,
    b.name                       as bairro,
    b.subprefeitura,
    b.ra                         as regiao_administrativa,
    os.postal_code               as cep,
    os.city                      as cidade,
    os.state                     as uf,
    os.latitude,
    os.longitude,
    os.geolocation_precise       as precisao_geo,
    os.favela_name               as favela,
    os.public_way_code           as codigo_logradouro,

    -- solicitante
    os.solicitante_nome,
    os.solicitante_telefone,
    c.nome_completo,
    c.cpf,
    c.person_email               as email,
    c.phone_1                    as telefone_1,
    c.phone_1_type               as telefone_1_tipo,
    c.phone_2                    as telefone_2,
    c.phone_3                    as telefone_3,

    -- protocolo
    p.status                     as protocolo_status,
    p.subject                    as protocolo_assunto,
    p.origin                     as protocolo_origem,
    p.priority                   as protocolo_prioridade,
    p.created_date               as protocolo_criado_em,
    p.closed_date                as protocolo_fechado_em,
    p.is_closed                  as protocolo_fechado,

    -- último andamento
    coalesce(a.qtd_andamentos, 0)  as qtd_andamentos,
    ult.ocorrido_em                as ultimo_andamento_em,
    ult.codigo                     as ultimo_andamento,
    ult.tipo                       as ultimo_andamento_tipo,
    ult.status_orgao               as ultimo_andamento_status,
    ult.situacao                   as ultimo_andamento_situacao,
    ult.informado_em               as ultimo_andamento_informado_em,
    ult.publicado_em               as ultimo_andamento_publicado_em,

    -- vínculos
    pai.work_order_number          as os_pai_numero,
    os.equivalent_count            as qtd_equivalentes,
    os.is_equivalencia             as e_equivalencia,

    -- formulário
    coalesce(r.qtd_respostas, 0)      as qtd_respostas,
    coalesce(r.respostas_json, '[]')  as respostas_json,
    coalesce(r.respostas_internas, 0) as respostas_internas

from os
left join protocolo p          on os.case_id              = p.id
left join servico s            on os.service              = s.id
left join uo                   on os.assigned_uo          = uo.id
left join usuario u            on os.owner_id             = u.id
left join fila f               on os.owner_id             = f.id
left join usuario criador      on os.created_by_id        = criador.id
left join bairro b             on os.neighborhood         = b.id
left join conta c              on os.account_id           = c.id
left join andamentos a         on os.id                   = a.os_id
left join ultimo_andamento ult on os.id                   = ult.os_id
left join respostas r          on os.id                   = r.os_id
left join os pai               on os.parent_work_order_id = pai.id

{% endmacro %}
