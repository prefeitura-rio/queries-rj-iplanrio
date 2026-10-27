{{
    config(
         materialized = 'ephemeral'
    )
}}

-- Nome frequencia acumulada dias letivos
with
    frequencia_cocs_fechados as (
        -- 1ª Parte: Totais salvos nos COCs anteriores ao atual
        select
            cast(aat.alu_id as int64) as alu_id,
            cast(cap.tpc_id as int64) as tpc_id,
            cast(cal.cal_ano as int64) as ano_calendario,
            cast(aat.aat_numeroaulas as int64) as numero_aulas,
            cast(aat.aat_numerofaltas as int64) as numero_faltas
        from {{ ref("gestao_escolar__aluno_avaliacao_turma") }} aat
        inner join
            {{ ref("gestao_escolar__aca_avaliacao") }} ava
            on aat.fav_id = ava.fav_id
            and aat.ava_id = ava.ava_id
            and ava.ava_tipo = 1
        inner join
            {{ ref("gestao_escolar__tur_turma") }} tur
            on aat.tur_id = tur.tur_id
            and tur.tur_situacao in (1, 5)
        inner join
            {{ ref("gestao_escolar__calendario_anual") }} cal on tur.cal_id = cal.cal_id
        inner join
            {{ ref("gestao_escolar__calendario_periodo") }} cap
            on tur.cal_id = cap.cal_id
            and ava.tpc_id = cap.tpc_id
            and cap.cap_datafim < current_date()  -- incluir filtro
        inner join
            {{ ref("gestao_escolar__mtr_matricula_turma") }} mtu
            on aat.alu_id = mtu.alu_id
            and aat.mtu_id = mtu.mtu_id
        -- aluno com mais de uma matrícula: vale a aat com dados (aulas não nulas) da
        -- matrícula ativa no fim do COC (situação 3 só vale se for a única); se nenhuma
        -- estiver ativa, a última que saiu antes do fim
        qualify
            row_number() over (
                partition by aat.alu_id, cap.tpc_id, cal.cal_ano
                order by
                    aat.aat_numeroaulas is not null desc,
                    mtu.mtu_situacao in (1, 5) desc,
                    (
                        mtu.mtu_dataMatricula <= cap.cap_datafim
                        and (mtu.mtu_dataSaida is null or mtu.mtu_dataSaida > cap.cap_datafim)
                    ) desc,
                    mtu.mtu_dataSaida desc,
                    aat.data_alteracao desc
            )
            = 1
    ),

    frequencia_coc_aberto as (
        -- 2ª Parte: Totais das aulas já realizadas no COC atual
        select
            alu_id,
            tpc_id,
            ano_calendario,
            sum(numero_aulas) as numero_aulas,
            sum(numero_faltas) as numero_faltas
        from
            (
                -- tipo 2: agrupa por dia (lógica original)
                select
                    alu_id,
                    tpc_id,
                    ano_calendario,
                    count(data_aula) as numero_aulas,
                    sum(
                        case when total_falta_tempo < total_tempos then 0 else 1 end
                    ) as numero_faltas
                from
                    (
                        select
                            cast(id_aluno as int64) as alu_id,
                            cast(id_tipo_calendario as int64) as tpc_id,
                            cast(tipo_frequencia_apurada as int64) as tipo_freq,
                            extract(year from data_aula) as ano_calendario,
                            data_aula,
                            sum(case when abonaFalta is true then 0 else falta end) as total_falta_tempo,
                            sum(numeroaulas) as total_tempos
                        from {{ ref("mart_frequencia__vw_alunos_aulas") }}
                        where tipo_frequencia_apurada = 2
                        group by
                            id_aluno,
                            id_tipo_calendario,
                            tipo_frequencia_apurada,
                            extract(year from data_aula),
                            data_aula
                    )
                group by alu_id, tpc_id, ano_calendario

                union all

                -- tipo 1: soma tempos sem agrupar por dia
                select
                    cast(id_aluno as int64) as alu_id,
                    cast(id_tipo_calendario as int64) as tpc_id,
                    extract(year from data_aula) as ano_calendario,
                    sum(numeroaulas) as numero_aulas,
                    sum(case when abonaFalta is true then 0 else falta end)  as numero_faltas
                from {{ ref("mart_frequencia__vw_alunos_aulas") }}
                where tipo_frequencia_apurada = 1
                group by id_aluno, id_tipo_calendario, extract(year from data_aula)
            )
        where ano_calendario = 2026 and tpc_id = 3
        group by alu_id, tpc_id, ano_calendario
    ),

    frequencia_acumulada_dias_letivos as (
        select *
        from frequencia_cocs_fechados
        union all
        select coc.*
        from frequencia_coc_aberto coc
        left join frequencia_cocs_fechados fcf
            on coc.alu_id = fcf.alu_id
            and coc.tpc_id = fcf.tpc_id
            and coc.ano_calendario = fcf.ano_calendario
        where fcf.alu_id is null
    ),

    final as (
        select

            {{
                dbt_utils.generate_surrogate_key(
                    ["alu_id", "tpc_id", "ano_calendario"]
                )
            }} as id,

            alu_id as id_aluno,
            tpc_id as id_tipo_calendario,
            ano_calendario,
            numero_faltas,
            numero_aulas,
            case
                when numero_aulas = 0
                then 0.00
                else
                    round(
                        100.00
                        - ((numero_faltas * 1.00) / (numero_aulas * 1.00) * 100.00),
                        2
                    )
            end as frequencia_percentual

        from frequencia_acumulada_dias_letivos
    ),

    correcoes_manuais as (select distinct * from final where numero_aulas is not null),

    correcao as (
        select *
        from correcoes_manuais
        where ano_calendario >= 2024
    ),
    -- Correção temporária: remoção dos alunos EJA para tratamento;
    vw_bi_aluno as (
        select alu_id as id_aluno, Grupamento as grupamento
        from {{ ref('gestao_escolar__vw_bi_aluno') }} alu
    )

select c.* 
from correcao c
left join vw_bi_aluno bia
on cast(c.id_aluno as string) = cast(bia.id_aluno as string)
where grupamento not like 'EJA%' and grupamento not like 'PROEJA%'