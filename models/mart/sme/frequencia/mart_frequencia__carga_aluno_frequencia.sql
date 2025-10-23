{{ config(
    alias='carga_aluno_frequencia'
) }}

with frqtrn as (
    select
        trn_id,
        count(*) / 5 as temposDia
    from {{ ref('raw_gestao_escolar__turno_horario') }}
    where trh_situacao = 1
    group by trn_id
), frqbase as (

    select
        alu_id,
        tpc_id,
        count(*) as registros,

        case when tpc_id = '1' then round(sum(freq_COC_1)/count(*), 2) else 0 end as freq_COC_1,
        case when tpc_id = '1' then sum(faltas_coc_1)/count(*) else 0 end as faltas_coc_1,
        case when tpc_id = '1' then sum(numeroAulas_coc_1)/count(*) else 0 end as numeroAulas_coc_1,

        case when tpc_id = '2' then round(sum(freq_COC_2)/count(*), 2) else 0 end as freq_COC_2,
        case when tpc_id = '2' then sum(faltas_coc_2)/count(*) else 0 end as faltas_coc_2,
        case when tpc_id = '2' then sum(numeroAulas_coc_2)/count(*) else 0 end as numeroAulas_coc_2,

        case when tpc_id = '3' then round(sum(freq_COC_3)/count(*), 2) else 0 end as freq_COC_3,
        case when tpc_id = '3' then sum(faltas_coc_3)/count(*) else 0 end as faltas_coc_3,
        case when tpc_id = '3' then sum(numeroAulas_coc_3)/count(*) else 0 end as numeroAulas_coc_3,

        case when tpc_id = '4' then round(sum(freq_COC_4)/count(*), 2) else 0 end as freq_COC_4,
        case when tpc_id = '4' then sum(faltas_coc_4)/count(*) else 0 end as faltas_coc_4,
        case when tpc_id = '4' then sum(numeroAulas_coc_4)/count(*) else 0 end as numeroAulas_coc_4,

        case when tpc_id = '5' then round(sum(freq_COC_5)/count(*), 2) else 0 end as freq_COC_5,
        case when tpc_id = '5' then sum(faltas_coc_5)/count(*) else 0 end as faltas_coc_5,
        case when tpc_id = '5' then sum(numeroAulas_coc_5)/count(*) else 0 end as numeroAulas_coc_5,

        sum(total_tempos)/count(*) as total_tempos,
        sum(total_faltas)/count(*) as total_faltas

    from (

        select
            frq.id_aluno as alu_id,
            frq.id_tipo_calendario as tpc_id,

            case when frq.id_tipo_calendario = '1' then round(((diasCoc * temposDia) - sum(IFNULL(faltas_disciplina_dia,0))) / CAST(diasCoc * temposDia AS FLOAT64) * 100, 2) else 0 end as freq_COC_1,
            case when frq.id_tipo_calendario = '1' then sum(IFNULL(faltas_disciplina_dia,0)) else 0 end as faltas_coc_1,
            case when frq.id_tipo_calendario = '1' then diasCoc * temposDia else 0 end as numeroAulas_coc_1,

            case when frq.id_tipo_calendario = '2' then round(((diasCoc * temposDia) - sum(IFNULL(faltas_disciplina_dia,0))) / CAST(diasCoc * temposDia AS FLOAT64) * 100, 2) else 0 end as freq_COC_2,
            case when frq.id_tipo_calendario = '2' then sum(IFNULL(faltas_disciplina_dia,0)) else 0 end as faltas_coc_2,
            case when frq.id_tipo_calendario = '2' then diasCoc * temposDia else 0 end as numeroAulas_coc_2,

            case when frq.id_tipo_calendario = '3' then round(((diasCoc * temposDia) - sum(IFNULL(faltas_disciplina_dia,0))) / CAST(diasCoc * temposDia AS FLOAT64) * 100, 2) else 0 end as freq_COC_3,
            case when frq.id_tipo_calendario = '3' then sum(IFNULL(faltas_disciplina_dia,0)) else 0 end as faltas_coc_3,
            case when frq.id_tipo_calendario = '3' then diasCoc * temposDia else 0 end as numeroAulas_coc_3,

            case when frq.id_tipo_calendario = '4' then round(((diasCoc * temposDia) - sum(IFNULL(faltas_disciplina_dia,0))) / CAST(diasCoc * temposDia AS FLOAT64) * 100, 2) else 0 end as freq_COC_4,
            case when frq.id_tipo_calendario = '4' then sum(IFNULL(faltas_disciplina_dia,0)) else 0 end as faltas_coc_4,
            case when frq.id_tipo_calendario = '4' then diasCoc * temposDia else 0 end as numeroAulas_coc_4,

            case when frq.id_tipo_calendario = '5' then round(((diasCoc * temposDia) - sum(IFNULL(faltas_disciplina_dia,0))) / CAST(diasCoc * temposDia AS FLOAT64) * 100, 2) else 0 end as freq_COC_5,
            case when frq.id_tipo_calendario = '5' then sum(IFNULL(faltas_disciplina_dia,0)) else 0 end as faltas_coc_5,
            case when frq.id_tipo_calendario = '5' then diasCoc * temposDia else 0 end as numeroAulas_coc_5,

            diasCoc * temposDia as total_tempos,
            sum(IFNULL(faltas_disciplina_dia,0)) as total_faltas

        from {{ ref('mart_frequencia__frequencia_mensal') }} frq

        inner join {{ ref('raw_gestao_escolar__aluno_curriculo') }} alc
            on alc.alu_id = frq.id_aluno
            and alc.esc_id = frq.id_escola
            and alc.alc_situacao <> 3
            and frq.data_aula between alc.alc_dataPrimeiraMatricula and IFNULL(alc.alc_dataSaida, CURRENT_DATE())

        inner join frqtrn trn
            on trn.trn_id = frq.id_turno

        inner join {{ ref('raw_educacao_basica_frequencia__diasCoc')}} dia
            on dia.cal_id = frq.id_ano_calendario and dia.tpc_id = frq.id_tipo_calendario

        where frq.efetivado = TRUE

        group by
            frq.id_aluno,
            frq.id_tipo_calendario,
            frq.id_ano_calendario,
            frq.id_turno,
            diasCoc,
            temposDia
    ) tab

    group by alu_id, tpc_id

), frqpri as (
    select
        alu_id,
        sum(total_tempos) as total_tempos,
        sum(total_faltas) as total_faltas,
        sum(freq_COC_1) as freq_coc1,
        sum(faltas_coc_1) as faltas_coc1,
        sum(numeroAulas_coc_1) as nm_aulas_coc1,
        sum(freq_COC_2) as freq_coc2,
        sum(faltas_coc_2) as faltas_coc2,
        sum(numeroAulas_coc_2) as nm_aulas_coc2,
        sum(freq_COC_3) as freq_coc3,
        sum(faltas_coc_3) as faltas_coc3,
        sum(numeroAulas_coc_3) as nm_aulas_coc3,
        sum(freq_COC_4) as freq_coc4,
        sum(faltas_coc_4) as faltas_coc4,
        sum(numeroAulas_coc_4) as nm_aulas_coc4,
        sum(freq_COC_5) as freq_coc5,
        sum(faltas_coc_5) as faltas_coc5,
        sum(numeroAulas_coc_5) as nm_aulas_coc5,
        case
            when sum(total_tempos) > 0 then
                round((sum(total_tempos) - sum(total_faltas)) / cast(sum(total_tempos) as float64) * 100, 2)
            else 0
        end as perc_freq_acumulada
    from frqbase
    group by alu_id
)

select
    frqpri.alu_id,
    alu.Ano,
    substr(esc.esc_codigo, 1, 2) as cre,
    esc.id_esc,
    esc.esc_codigo,
    alu.cpf,
    frqpri.total_tempos,
    frqpri.total_faltas,
    round(frqpri.perc_freq_acumulada, 2) as perc_freq_acumulada,
    round(100 - frqpri.perc_freq_acumulada, 2) as perc_faltas_acumulada,
    freq_coc1,
    faltas_coc1,
    nm_aulas_coc1,
    freq_coc2,
    faltas_coc2,
    nm_aulas_coc2,
    freq_coc3,
    faltas_coc3,
    nm_aulas_coc3,
    freq_coc4,
    faltas_coc4,
    nm_aulas_coc4,
    freq_coc5,
    faltas_coc5,
    nm_aulas_coc5
from {{ ref('raw_gestao_escolar__vw_bi_aluno') }} alu
inner join {{ ref('raw_gestao_escolar__tur_turma') }} tur on alu.tur_id = tur.tur_id and tur.tur_situacao = 1
inner join {{ ref('raw_gestao_escolar__esc_escola') }} esc on esc.id_esc = tur.esc_id and esc.esc_situacao = 1
inner join frqpri on frqpri.alu_id = alu.alu_id

