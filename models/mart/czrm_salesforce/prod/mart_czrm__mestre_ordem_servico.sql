{{ config(
    alias="mestre_ordem_servico",
    materialized="table",
    partition_by={"field": "criado_em", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["status", "servico"],
    on_schema_change="sync_all_columns",
) }}

WITH
    os AS (
        SELECT
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
        FROM
            {{ref ('raw_czrm__work_order')}}
    ),
    protocolo AS (
        SELECT
            id,
            case_number,
            status,
            subject,
            origin,
            priority,
            created_date,
            closed_date,
            is_closed
        FROM
            {{ref ('raw_czrm__service_case')}}
    ),
    servico AS (
        SELECT
            id,
            service_name,
            theme,
            subtheme
        FROM
            {{ref ('raw_czrm__service_catalog')}}
    ),
    uo AS (
        SELECT
            id,
            shortname,
            name
        FROM
            {{ref ('raw_czrm__organizational_unity')}}
    ),
    usuario AS (
        SELECT
            id,
            name
        FROM
            {{ref ('raw_czrm__user')}}
    ),
    fila AS (
        SELECT
            id,
            name
        FROM
            {{ref ('raw_czrm__queue_group')}}
    ),
    bairro AS (
        SELECT
            id,
            name,
            subprefeitura,
            ra
        FROM
            {{ref ('raw_czrm__neighborhood')}}
    ),
    conta AS (
        SELECT
            id,
            nome_completo,
            cpf,
            person_email,
            phone_1,
            phone_1_type,
            phone_2,
            phone_3
        FROM
            {{ref ('raw_czrm__account')}}
    ),
    -- uma passada: contagem + último andamento, sem array_agg/sort agregado
    andamentos AS (
        SELECT
            work_order AS os_id,
            qtd_andamentos,
            struct(titulo, tipo, status, data_hora, publicado_em) AS ultimo
        FROM
            (
                SELECT
                    work_order,
                    name AS titulo,
                    event_type AS tipo,
                    status_normalizado AS status,
                    data_hora_andamento AS data_hora,
                    publication_datetime AS publicado_em,
                    count(*) over (
                        PARTITION BY
                            work_order
                    ) AS qtd_andamentos,
                    row_number() over (
                        PARTITION BY
                            work_order
                        ORDER BY
                            data_hora_andamento DESC
                    ) AS rn
                FROM
                    {{ref ('raw_czrm__work_order_update')}}
            )
        WHERE
            rn = 1
    ),
    -- emite JSON direto, sem string -> parse_json
    respostas AS (
        SELECT
            work_order AS os_id,
            count(*) AS qtd_respostas,
            countif (is_internal) AS respostas_internas,
            to_json(
                array_agg(
                    struct(attribute_label AS campo, value_display AS valor)
                )
            ) AS respostas_json
        FROM
            {{ref ('raw_czrm__work_order_attribute_answer')}}
        GROUP BY
            work_order
    )
SELECT
    -- chaves
    cast(os.id AS string) AS os_id,
    cast(os.work_order_number AS string) AS numero_os,
    cast(p.case_number AS string) AS numero_protocolo,
    cast(os.created_date AS timestamp) AS criado_em,
    cast(os.system_modstamp AS timestamp) AS atualizado_em,
    -- status e classificação
    cast(os.status AS string) AS status,
    cast(os.is_wo_closed AS boolean) AS encerrada,
    cast(os.priority AS string) AS prioridade,
    cast(os.canal_atendimento AS string) AS canal_atendimento,
    cast(os.tipo_solicitante AS string) AS tipo_solicitante,
    cast(os.close_reason AS string) AS motivo_encerramento,
    cast(os.categoria AS string) AS tipo_categoria_servico,
    cast(os.description AS string) AS descricao_wo,
    -- serviço, UO e responsáveis
    cast(s.service_name AS string) AS servico,
    cast(s.theme AS string) AS servico_tema,
    cast(s.subtheme AS string) AS servico_subtema,
    cast(uo.shortname AS string) AS uo_sigla,
    cast(uo.name AS string) AS uo_nome,
    cast(os.main_organizational_unit AS string) AS orgao_central,
    cast(coalesce(u.name, f.name) AS string) AS dono_nome,
    cast(f.name AS string) AS fila,
    cast(criador.name AS string) AS criado_por,
    -- datas
    cast(os.data_fechamento AS timestamp) AS fechada_em,
    cast(os.sla_target_date AS timestamp) AS sla_prazo,
    -- local
    cast(os.public_way AS string) AS logradouro,
    cast(os.door_number AS string) AS numero,
    cast(os.no_number AS boolean) AS sem_numero,
    cast(os.complement AS string) AS complemento,
    cast(os.landmark AS string) AS referencia,
    cast(b.name AS string) AS bairro,
    cast(b.subprefeitura AS string) AS subprefeitura,
    cast(b.ra AS string) AS regiao_administrativa,
    cast(os.postal_code AS string) AS cep,
    cast(os.city AS string) AS cidade,
    cast(os.state AS string) AS uf,
    cast(os.latitude AS float64) AS latitude,
    cast(os.longitude AS float64) AS longitude,
    cast(os.geolocation_precise AS boolean) AS precisao_geo,
    cast(os.favela_name AS string) AS favela,
    cast(os.public_way_code AS string) AS codigo_logradouro,
    -- solicitante
    cast(os.solicitante_nome AS string) AS solicitante_nome,
    cast(os.solicitante_telefone AS string) AS solicitante_telefone,
    cast(c.nome_completo AS string) AS nome_completo,
    cast(c.cpf AS string) AS cpf,
    cast(c.person_email AS string) AS email,
    cast(c.phone_1 AS string) AS telefone_1,
    cast(c.phone_1_type AS string) AS telefone_1_tipo,
    cast(c.phone_2 AS string) AS telefone_2,
    cast(c.phone_3 AS string) AS telefone_3,
    -- protocolo
    cast(p.status AS string) AS protocolo_status,
    cast(p.subject AS string) AS protocolo_assunto,
    cast(p.origin AS string) AS protocolo_origem,
    cast(p.priority AS string) AS protocolo_prioridade,
    cast(p.created_date AS timestamp) AS protocolo_criado_em,
    cast(p.closed_date AS timestamp) AS protocolo_fechado_em,
    cast(p.is_closed AS boolean) AS protocolo_fechado,
    -- último andamento
    cast(coalesce(a.qtd_andamentos, 0) AS int64) AS qtd_andamentos,
    cast(a.ultimo.data_hora AS timestamp) AS ultimo_andamento_em,
    cast(a.ultimo.titulo AS string) AS ultimo_andamento,
    cast(a.ultimo.tipo AS string) AS ultimo_andamento_tipo,
    cast(a.ultimo.status AS string) AS ultimo_andamento_status,
    cast(a.ultimo.publicado_em AS timestamp) AS ultimo_andamento_publicado_em,
    -- vínculos
    cast(pai.work_order_number AS string) AS os_pai_numero,
    cast(os.equivalent_count AS int64) AS qtd_equivalentes,
    cast(os.is_equivalencia AS boolean) AS e_equivalencia,
    -- formulário
    cast(coalesce(r.qtd_respostas, 0) AS int64) AS qtd_respostas,
    coalesce(r.respostas_json, json '[]') AS respostas_json,
    cast(coalesce(r.respostas_internas, 0) AS int64) AS respostas_internas
FROM
    os
    LEFT JOIN protocolo p ON os.case_id = p.id
    LEFT JOIN servico s ON os.service = s.id
    LEFT JOIN uo ON os.assigned_uo = uo.id
    LEFT JOIN usuario u ON os.owner_id = u.id
    LEFT JOIN fila f ON os.owner_id = f.id
    LEFT JOIN usuario criador ON os.created_by_id = criador.id
    LEFT JOIN bairro b ON os.neighborhood = b.id
    LEFT JOIN conta c ON os.account_id = c.id
    LEFT JOIN andamentos a ON os.id = a.os_id
    LEFT JOIN respostas r ON os.id = r.os_id
    LEFT JOIN os pai ON os.parent_work_order_id = pai.id