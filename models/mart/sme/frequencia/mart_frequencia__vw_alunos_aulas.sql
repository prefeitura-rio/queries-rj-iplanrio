{{
    config(
        alias='vw_alunos_aulas',
    )
}}

with alunos_aulas as (
    select
        taa.id_aluno,    -- identificador único do aluno, tabela origem ACA_Aluno
        taa.id_matricula_turma,    -- identificador único da matricula do aluno na turma
        taa.id_matricula_disciplina,    -- identificador único da matricula do aluno na disciplina em dada turma
        taa.id_disciplina_turma,    -- identificador único da disciplina na turma
        tau.id_tipo_calendario,    -- identificador único do conselho de classe (COC)
        tau.data_aula,  -- data na qual houve aula realizada e com frequencia efetivada pelo professor
        case fav.tipo_frequencia_apurada -- se a frequência for apurada por aula planejada, respeita a quantidade de faltas registradas. Se a frequência estiver sendo apurada por período, considera 1 falta para os casos que tiver falta registrada > 1
            when 1 then SAFE_CAST(coalesce(taa.faltas_disciplina_dia, 0) AS INT64)
            else
                case when coalesce(taa.faltas_disciplina_dia, 0) > 1
                    THEN 1
                    ELSE 0
                END
        end as falta,   -- número de faltas no dia (tau_data), nunca maior que o numeroAulas
--        SAFE_CAST(coalesce(taa.faltas_disciplina_dia, 0) AS INT64) as falta,   -- número de faltas no dia (tau_data), nunca maior que o numeroAulas
        SAFE_CAST(coalesce(taa.frequencia_tempo, '0') AS INT64) as taa_frequenciaBitMap, -- string com o registro de presença e falta (1-falta; 0-presença)
        case fav.tipo_frequencia_apurada
            when 1 then LENGTH(taa.frequencia_tempo)
            else 1
        end as numeroAulas,                        -- número de aulas ou tempos de aula
        fav.tipo_frequencia_apurada,         -- parametro que indica se numeroAulas 1-será o tempo da aula ou 2-se será o dia de aula
        coalesce(tjf.tjf_abonaFalta, FALSE) as abonaFalta -- informa que houve abono no dia
    from {{ ref('raw_gestao_escolar__turma_aula_aluno') }} as taa
    inner join {{ ref('raw_gestao_escolar__turma_aula') }} as tau
        on taa.id_disciplina_turma = tau.id_disciplina
        and taa.id_aula_disciplina = tau.id_aula_disciplina       -- identificador da aula em uma dada turma discilina - sequencial que compoe a chave da TurmaAula
        and taa.id_situacao in ('1', '4', '6')   -- situaçao do registro = 1-ativo; 3-excluído; 6-cancelado; 4-???
        and tau.id_situacao in ('1', '4', '6')   -- situaçao do registro = 1-ativo; 3-excluído; 6-cancelado; 4-???
        and tau.efetivado = TRUE         -- indica aula cuja a frequencia foi confirmada pelo professor (1-efetivado; 0-não efetivada)
    inner join {{ ref('raw_gestao_escolar__mtr_matricula_turma') }} as mtu
        on taa.id_aluno = mtu.alu_id
        and taa.id_matricula_turma = mtu.mtu_id
        and mtu.mtu_situacao in (1, 5)   -- situação da matricula (1-ativo;5-ativo com movimentação)
        -- considera a frequencia somente as aulas dentro do período de matricula do aluno, inclusive na entrada e exclusive na saída
        and tau.data_aula >= mtu.mtu_dataMatricula
        and tau.data_aula < coalesce(mtu.mtu_dataSaida, current_date())  -- apuração da frequencia desconsidera o dia de saida da turma na movimentacao
    inner join {{ ref('raw_gestao_escolar__tur_turma') }} as tur
        on mtu.tur_id = tur.tur_id
    inner join {{ ref('raw_gestao_escolar__formato_avaliacao') }} as fav
        on tur.fav_id = fav.id_formato_avaliacao   -- para obter o parametro fav_tipoApuracaoFrequencia
    -- para verificar a justificativa que abona as faltas no dia
    left join {{ ref('raw_gestao_escolar__aluno_justificativa_falta') }} as afj
        on afj.alu_id = taa.id_aluno
        and afj.afj_situacao <> 3       -- situação diferente de registro excluído
        and tau.data_aula >= afj.afj_dataInicio
        and (afj.afj_dataFim is null or tau.data_aula <= afj.afj_dataFim)
    left join {{ ref('raw_gestao_escolar__tipo_justificativa_falta') }} as tjf
        on tjf.tjf_id = afj.tjf_id
        and tjf.tjf_situacao <> 3       -- situação diferente de registro excluído
    -- para excluír os dias de feriado
    left join {{ source('brutos_core_sso_staging', 'SYS_DiaNaoUtil') }} as dnu
        on (
            tau.data_aula = SAFE_CAST(dnu.dnu_data AS DATE) -- se existe feriado no mesmo dia da aula planejada
            or (
                dnu.dnu_recorrencia = '1'   -- se o feriado acontece todo ano, verificar somente mes e dia do feriado
                and extract(month from tau.data_aula) = extract(month from SAFE_CAST(dnu.dnu_data AS DATE))
                and extract(day from tau.data_aula) = extract(day from SAFE_CAST(dnu.dnu_data AS DATE))
            )
        )
        and dnu.dnu_situacao = '1'   -- situacao do registro 1 = ativo; 3=excluído
    where dnu.dnu_data is null
)


select * from alunos_aulas
