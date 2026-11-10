{{
    config(
         materialized = 'ephemeral'
    )
}}

-- Nome frequencia acumulada dias letivos
with
    -- turmas e matrículas da EJA: calendário "Educação Jovens e Adultos" de cada ano
    turmas_eja as (
        select tur.tur_id
        from {{ ref("gestao_escolar__tur_turma") }} tur
        inner join
            {{ ref("gestao_escolar__calendario_anual") }} cal on tur.cal_id = cal.cal_id
        where regexp_contains(cal.cal_descricao, r"Jovens e Adultos")
    ),

    matriculas_eja as (
        -- a matrícula é identificada por aluno + mtu_id (mtu_id é sequencial por aluno)
        select distinct mtu.alu_id, mtu.mtu_id
        from {{ ref("gestao_escolar__mtr_matricula_turma") }} mtu
        inner join turmas_eja te on mtu.tur_id = te.tur_id
    ),

    aat_cocs_fechados as (
        -- 1ª Parte: avaliações (aat) dos COCs anteriores ao atual
        select
            aat.alu_id,
            cap.tpc_id,
            cal.cal_ano,
            aat.mtu_id,
            aat.aat_numeroaulas,
            aat.aat_numerofaltas,
            aat.aat_situacao,
            aat.aat_frequencia,
            aat.aat_frequenciaacumuladacalculada,
            aat.data_alteracao,
            mtu.mtu_situacao,
            mtu.mtu_dataMatricula,
            mtu.mtu_dataSaida,
            cap.cap_datafim,
            -- par aluno/COC é EJA quando todas as suas aat são de turma da EJA
            logical_and(regexp_contains(cal.cal_descricao, r"Jovens e Adultos")) over (
                partition by aat.alu_id, cap.tpc_id, cal.cal_ano
            ) as par_eja
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
    ),

    frequencia_cocs_fechados_regular as (
        select
            cast(alu_id as int64) as alu_id,
            cast(tpc_id as int64) as tpc_id,
            cast(cal_ano as int64) as ano_calendario,
            cast(aat_numeroaulas as int64) as numero_aulas,
            cast(aat_numerofaltas as int64) as numero_faltas,
            cast(null as float64) as frequencia_gravada
        from aat_cocs_fechados
        where not par_eja
        -- aluno com mais de uma matrícula: vale a aat com dados (aulas não nulas) da
        -- matrícula ativa no fim do COC (situação 3 só vale se for a única); se nenhuma
        -- estiver ativa, a última que saiu antes do fim
        qualify
            row_number() over (
                partition by alu_id, tpc_id, cal_ano
                order by
                    aat_numeroaulas is not null desc,
                    mtu_situacao in (1, 5) desc,
                    (
                        mtu_dataMatricula <= cap_datafim
                        and (mtu_dataSaida is null or mtu_dataSaida > cap_datafim)
                    ) desc,
                    mtu_dataSaida desc,
                    data_alteracao desc
            )
            = 1
    ),

    frequencia_cocs_fechados_eja as (
        -- EJA (regra da SME): ignora avaliação cancelada (aat_situacao = 3), vale a aat
        -- da matrícula de maior mtu_id e a frequência gravada no SGA (aat_frequencia);
        -- se ela for nula ou 0,00 (caso de aula zero), vale a frequência acumulada
        select
            cast(alu_id as int64) as alu_id,
            cast(tpc_id as int64) as tpc_id,
            cast(cal_ano as int64) as ano_calendario,
            cast(aat_numeroaulas as int64) as numero_aulas,
            cast(aat_numerofaltas as int64) as numero_faltas,
            case
                when coalesce(safe_cast(aat_frequencia as float64), 0) = 0
                then safe_cast(aat_frequenciaacumuladacalculada as float64)
                else safe_cast(aat_frequencia as float64)
            end as frequencia_gravada
        from aat_cocs_fechados
        where par_eja and aat_situacao is distinct from '3'
        qualify
            row_number() over (
                partition by alu_id, tpc_id, cal_ano
                order by safe_cast(mtu_id as int64) desc, data_alteracao desc
            )
            = 1
    ),

    frequencia_cocs_fechados as (
        select *
        from frequencia_cocs_fechados_regular
        union all
        select *
        from frequencia_cocs_fechados_eja
    ),

    alunos_aulas as (
        select aa.*
        from {{ ref("mart_frequencia__vw_alunos_aulas") }} aa
        where
            -- COC aberto do calendário regular
            cast(aa.id_tipo_calendario as int64) = 4  -- ALTERAR EM TODA VIRADA DE COC
            -- COC aberto da EJA, só nas matrículas de turma da EJA
            or (
                cast(aa.id_tipo_calendario as int64) = 3  -- ALTERAR EM TODA VIRADA DE COC
                and exists (
                    select 1
                    from matriculas_eja me
                    where me.alu_id = aa.id_aluno and me.mtu_id = aa.id_matricula_turma
                )
            )
    ),

    frequencia_coc_aberto as (
        -- 2ª Parte: Totais das aulas já realizadas no COC atual
        select
            alu_id,
            tpc_id,
            ano_calendario,
            sum(numero_aulas) as numero_aulas,
            sum(numero_faltas) as numero_faltas,
            cast(null as float64) as frequencia_gravada
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
                        from alunos_aulas
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
                from alunos_aulas
                where tipo_frequencia_apurada = 1
                group by id_aluno, id_tipo_calendario, extract(year from data_aula)
            )
        where ano_calendario = 2026 and tpc_id in (3, 4)  -- ALTERAR EM TODA VIRADA DE COC
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
            coalesce(
                -- EJA: frequência gravada no SGA
                round(frequencia_gravada, 2),
                case
                    when numero_aulas = 0
                    then 0.00
                    else
                        round(
                            100.00
                            - ((numero_faltas * 1.00) / (numero_aulas * 1.00) * 100.00),
                            2
                        )
                end
            ) as frequencia_percentual,
            frequencia_gravada

        from frequencia_acumulada_dias_letivos
    ),

    correcoes_manuais as (
        select distinct * except (frequencia_gravada)
        from final
        where numero_aulas is not null or frequencia_gravada is not null
    ),

    correcao as (
        select *
        from correcoes_manuais
        where ano_calendario >= 2024
    )

select *
from correcao
