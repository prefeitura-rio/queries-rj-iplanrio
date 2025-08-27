{{ config(
    alias='frequencia_acumulada_dias_letivos',
) }}

WITH frequencia_acumulada AS (
    -- Primeira parte: Frequência dos COCs anteriores
    SELECT
        AAT.alu_id,
        --AAT.mtu_id, -- ??? qual o motivo? trazer uma linha por aluno ao inves de aluno/turma
        CAP.tpc_id,
        AAT.aat_numeroAulas AS numeroAulas,
        AAT.aat_numeroFaltas AS numeroFaltas
    FROM {{ ref('raw_gestao_escolar__aluno_avaliacao_turma') }} AAT
    INNER JOIN {{ ref('raw_gestao_escolar__avaliacao') }} AVA
        ON AAT.fav_id = AVA.fav_id
        AND AAT.ava_id = AVA.ava_id
    INNER JOIN {{ ref('raw_gestao_escolar__tur_turma') }} TUR
        ON AAT.tur_id = TUR.tur_id
        AND TUR.tur_situacao IN (1,5)
    INNER JOIN {{ ref('raw_gestao_escolar__calendario_anual') }} CAL
        ON TUR.cal_id = CAL.cal_id
        AND CAL.cal_ano = EXTRACT(YEAR FROM CURRENT_DATE())
    INNER JOIN {{ ref('raw_gestao_escolar__calendario_periodo') }} CAP
        ON TUR.cal_id = CAP.cal_id
        AND CAP.cap_id = AVA.tpc_id
        AND CAP.cap_dataFim < CURRENT_DATE()

    UNION ALL

    -- Segunda parte: Frequência do COC atual usando o modelo numeroDeAulasCte
    SELECT
        NUM.alu_id,
        NUM.tpc_id,
        NUM.numeroAulas,
		SUM(SQ_FALTAS.num_faltas) AS numeroFaltas
    FROM {{ ref('raw_gestao_escolar__numeroDeAulasCte') }} NUM
    LEFT JOIN (
        -- Subquery para calcular faltas do COC atual
        SELECT
            MTU.alu_id,
            TAU.id_tipo_calendario,
            --MTU.mtu_id,
            SUM(TAA.faltas_disciplina_dia) AS num_faltas
        FROM {{ ref('raw_gestao_escolar__turma_aula_aluno') }} TAA
        INNER JOIN {{ ref('raw_gestao_escolar__turma_aula') }} TAU
            ON TAA.id_disciplina_turma = TAU.id_disciplina
            AND TAA.id_aula_disciplina = TAU.id_aula_disciplina
            AND TAA.id_situacao IN ('1','4','6')
            AND TAU.id_situacao IN ('1','4','6')
            AND TAU.efetivado = TRUE
            AND TAA.faltas_disciplina_dia > 0
        INNER JOIN {{ ref('raw_gestao_escolar__mtr_matricula_turma') }} MTU
            ON TAA.id_aluno = MTU.alu_id
            AND TAA.id_matricula_turma = MTU.mtu_id
            AND MTU.mtu_situacao IN (1,5)
            AND TAU.data_aula >= MTU.mtu_dataMatricula
            AND TAU.data_aula < COALESCE(MTU.mtu_dataSaida, CURRENT_DATE())
        INNER JOIN {{ ref('raw_gestao_escolar__tur_turma') }} TUR
            ON MTU.tur_id = TUR.tur_id
        LEFT JOIN {{ ref('raw_gestao_escolar__aluno_justificativa_falta') }} AFJ
            ON AFJ.alu_id = TAA.id_aluno
            AND AFJ.afj_situacao != 3
            AND TAU.data_aula >= AFJ.afj_dataInicio
            AND (AFJ.afj_dataFim IS NULL OR TAU.data_aula <= AFJ.afj_dataFim)
        LEFT JOIN {{ ref('raw_gestao_escolar__tipo_justificativa_falta') }} TJF
            ON TJF.tjf_id = AFJ.tjf_id
            AND TJF.tjf_situacao != 3
        LEFT JOIN {{ source('brutos_core_sso_staging', 'SYS_DiaNaoUtil') }} DNU
            ON (
                TAU.data_aula = CAST(DNU.dnu_data AS DATE)
                OR
                (DNU.dnu_recorrencia = '1'
                    AND EXTRACT(MONTH FROM TAU.data_aula) = EXTRACT(MONTH FROM CAST(DNU.dnu_data AS DATE))
                    AND EXTRACT(DAY FROM TAU.data_aula) = EXTRACT(DAY FROM CAST(DNU.dnu_data AS DATE))
                )
            )
            AND DNU.dnu_situacao = '1'
        WHERE
            DNU.dnu_data IS NULL
            AND TJF.tjf_abonaFalta IS NULL
        GROUP BY
            MTU.alu_id,
           -- MTU.mtu_id,
            TAU.id_tipo_calendario
    ) SQ_FALTAS
        ON NUM.alu_id = SQ_FALTAS.alu_id
        --AND MTU.mtu_id = SQ_FALTAS.mtu_id
        AND SQ_FALTAS.id_tipo_calendario = NUM.tpc_id
    GROUP BY
        NUM.alu_id,
        NUM.tpc_id,
        NUM.numeroAulas
), sum_cte AS (
    SELECT
         alu_id,
        --mtu_id,
        tpc_id,
        COALESCE(SUM(numeroFaltas),0) numeroFaltas,
        COALESCE(SUM(numeroAulas),0) numeroAulas,
    FROM frequencia_acumulada
	GROUP BY alu_id, tpc_id
)

SELECT
    alu_id AS id_aluno,
    tpc_id AS id_tipo_calendario,
    numeroFaltas AS numero_faltas,
    numeroAulas AS numero_aulas,
    CASE
        WHEN numeroAulas = 0 THEN 0.00
        ELSE ROUND(100.00 - ((numeroFaltas * 1.00) / (numeroAulas * 1.00) * 100.00), 2)
    END AS frequencia_percentual

FROM sum_cte
ORDER BY alu_id