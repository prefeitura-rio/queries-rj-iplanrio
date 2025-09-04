{{
  config(
    alias='frequencia_acumulada',
  )
}}

-- Modelo que calcula o indicador de frequência acumulada por aluno
-- a partir do início do ano letivo até a data atual, considerando
-- o período de matrícula do aluno na(s) turma(s)


WITH frequencia_acumulada as (
    select
        vaa.id_aluno,
        sum(vaa.falta) as numero_faltas,
        sum(vaa.numeroAulas) as numero_aulas
    from {{ ref('mart_frequencia__vw_alunos_aulas') }} vaa
    inner join {{ ref('raw_gestao_escolar__mtr_matricula_turma') }} mtu
        on vaa.id_aluno = mtu.alu_id
        and vaa.id_matricula_turma = mtu.mtu_id
    inner join {{ ref('raw_gestao_escolar__tur_turma') }} tur
        on mtu.tur_id = tur.tur_id
    inner join {{ ref('raw_gestao_escolar__calendario_anual') }} cal
        on tur.cal_id = cal.cal_id
        and cal.cal_ano = EXTRACT(YEAR FROM CURRENT_DATE() )   -- limita a busca ao ano corrente
    where vaa.data_aula >= cal.cal_dataInicio
        and vaa.abonaFalta = FALSE  -- não considera dias com abono de falta
    group by vaa.id_aluno
)

select
    id_aluno,
    numero_faltas,
    numero_aulas,
    ROUND(100.00 - ((numero_faltas * 1.00) / (numero_aulas * 1.00) * 100.00), 2) as frequencia_percentual
from frequencia_acumulada
