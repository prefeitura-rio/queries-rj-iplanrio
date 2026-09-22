{{
    config(
        alias="frequencia_acumulada_dias_letivos",
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
            cast(aat.aat_numeroaulas as int64) as numeroaulas,
            cast(aat.aat_numerofaltas as int64) as numerofaltas
        from {{ ref("gestao_escolar__aluno_avaliacao_turma") }} aat
        inner join
            {{ ref("gestao_escolar__aca_avaliacao") }} ava
            on aat.fav_id = ava.fav_id
            and aat.ava_id = ava.ava_id
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
    ),

    frequencia_coc_atual as (
        -- 2ª Parte: Totais das aulas já realizadas no COC atual (consome a view
        -- consolidada)
        select
            cast(id_aluno as int64) as alu_id,
            cast(id_tipo_calendario as int64) as tpc_id,
            extract(year from data_aula) as ano_calendario,
            sum(numeroaulas) as numeroaulas,
            sum(falta) as numerofaltas
        from {{ ref("mart_frequencia__vw_alunos_aulas") }}

        group by id_aluno, id_tipo_calendario, extract(year from data_aula)
        having ano_calendario = 2026 and tpc_id = 3 -- ALTERAR EM TODA VIRADA DE COC

    ),

    frequencia_acumulada_dias_letivos as (
        select *
        from frequencia_cocs_fechados
        union all
        select *
        from frequencia_coc_atual
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
            numerofaltas as numero_faltas,
            numeroaulas as numero_aulas,
            case
                when numeroaulas = 0
                then 0.00
                else
                    round(
                        100.00
                        - ((numerofaltas * 1.00) / (numeroaulas * 1.00) * 100.00),
                        2
                    )
            end as frequencia_percentual

        from frequencia_acumulada_dias_letivos
    ),

    correcoes_manuais as (select distinct * from final where numero_aulas is not null)

select *
from correcoes_manuais
where ano_calendario >= 2024
order by id_aluno, ano_calendario desc, id_tipo_calendario desc
