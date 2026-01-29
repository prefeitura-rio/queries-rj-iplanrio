{{ config(
    alias='frequencia_acumulada_dias_letivos_2025',
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
        AND CAL.cal_ano = 2025
    INNER JOIN {{ ref('raw_gestao_escolar__calendario_periodo') }} CAP
        ON TUR.cal_id = CAP.cal_id
        AND CAP.cap_id = AVA.tpc_id
        AND CAP.cap_dataFim < DATE('2026-01-01')

    UNION ALL

    -- Segunda parte: Frequência do COC atual usando o modelo numeroDeAulasCte
    SELECT
        NUM.alu_id,
        NUM.tpc_id,
        NUM.numeroAulas,
		SUM(SQ_FALTAS.num_faltas) AS numeroFaltas
    FROM {{ ref('raw_gestao_escolar__numeroDeAulasCte') }} NUM
    LEFT JOIN (
        SELECT
            MTU.alu_id
            , SQ_TA.id_tipo_calendario 
            , MTU.mtu_id -- INSERIDO EM 18/07/25 PARA RELACIONAMENTO
            , SUM(SQ_TA.taa_frequencia) num_faltas
        FROM
            (
        SELECT
            TAA.id_aluno,
            TAA.id_matricula_turma,
            TAU.data_aula,
            TAU.id_tipo_calendario,
            FAV.tipo_frequencia_apurada,
            MIN(COALESCE(TAA.faltas_disciplina_dia, 0)) AS taa_frequencia
        FROM {{ ref('raw_gestao_escolar__turma_aula_aluno') }} TAA
        INNER JOIN {{ ref('raw_gestao_escolar__turma_aula') }} TAU
            ON TAA.id_disciplina_turma = TAU.id_disciplina
            AND TAA.id_aula_disciplina = TAU.id_aula_disciplina
            AND TAA.id_situacao IN ('1','4','6')
            AND TAU.id_situacao IN ('1','4','6')
            AND TAU.efetivado = TRUE
            AND TAA.faltas_disciplina_dia > 0
            AND EXTRACT(YEAR FROM TAU.data_aula) = 2025
        INNER JOIN {{ ref('raw_gestao_escolar__mtr_matricula_turma') }} MTU
            ON TAA.id_aluno = MTU.alu_id
            AND TAA.id_matricula_turma = MTU.mtu_id
        INNER JOIN {{ ref('raw_gestao_escolar__tur_turma') }} TUR
            ON MTU.tur_id = TUR.tur_id
        INNER JOIN {{ ref('raw_gestao_escolar__formato_avaliacao') }} FAV
            ON TUR.fav_id = FAV.id_formato_avaliacao   
            AND FAV.tipo_frequencia_apurada = 2
        GROUP BY
            TAA.id_aluno,
            TAA.id_matricula_turma,
            TAU.data_aula,
            TAU.id_tipo_calendario,
            FAV.tipo_frequencia_apurada


        UNION ALL

        SELECT
            TAA.id_aluno,
            TAA.id_matricula_turma,
            TAU.data_aula,
            TAU.id_tipo_calendario,
            FAV.tipo_frequencia_apurada,
            MIN(COALESCE(TAA.faltas_disciplina_dia, 0)) AS taa_frequencia
        FROM {{ ref('raw_gestao_escolar__turma_aula_aluno') }} TAA
        INNER JOIN {{ ref('raw_gestao_escolar__turma_aula') }} TAU
            ON TAA.id_disciplina_turma = TAU.id_disciplina
            AND TAA.id_aula_disciplina = TAU.id_aula_disciplina
            AND TAA.id_situacao IN ('1','4','6')
            AND TAU.id_situacao IN ('1','4','6')
            AND TAU.efetivado = TRUE
            AND TAA.faltas_disciplina_dia > 0
            AND EXTRACT(YEAR FROM TAU.data_aula) = 2025
        INNER JOIN {{ ref('raw_gestao_escolar__mtr_matricula_turma') }} MTU
            ON TAA.id_aluno = MTU.alu_id
            AND TAA.id_matricula_turma = MTU.mtu_id
        INNER JOIN {{ ref('raw_gestao_escolar__tur_turma') }} TUR
            ON MTU.tur_id = TUR.tur_id
        INNER JOIN {{ ref('raw_gestao_escolar__formato_avaliacao') }} FAV
            ON TUR.fav_id = FAV.id_formato_avaliacao   
            AND FAV.tipo_frequencia_apurada = 1
        GROUP BY
            TAA.id_aluno,
            TAA.id_matricula_turma,
            TAU.data_aula,
            TAU.id_tipo_calendario,
            FAV.tipo_frequencia_apurada

    ) SQ_TA
    INNER JOIN {{ ref('raw_gestao_escolar__mtr_matricula_turma') }} MTU
            ON SQ_TA.id_aluno = MTU.alu_id
            AND SQ_TA.id_matricula_turma = MTU.mtu_id
            AND MTU.mtu_situacao IN (1,5)
            AND SQ_TA.data_aula >= MTU.mtu_dataMatricula
            AND SQ_TA.data_aula < COALESCE(MTU.mtu_dataSaida, DATE('2026-01-01'))
            AND EXTRACT(YEAR FROM SQ_TA.data_aula) = 2025
        INNER JOIN {{ ref('raw_gestao_escolar__tur_turma') }} TUR
            ON MTU.tur_id = TUR.tur_id
            AND TUR.tur_situacao IN (1,5)
        INNER JOIN {{ ref('raw_gestao_escolar__calendario_anual') }} CAL
            ON TUR.cal_id = CAL.cal_id 
            AND CAL.cal_ano = 2025
        INNER JOIN {{ ref('raw_gestao_escolar__formato_avaliacao') }} FAV
            ON TUR.fav_id = FAV.id_formato_avaliacao   
        LEFT JOIN {{ ref('raw_gestao_escolar__aluno_justificativa_falta') }} AFJ
            ON AFJ.alu_id = SQ_TA.id_aluno
            AND AFJ.afj_situacao != 3
            AND data_aula >= AFJ.afj_dataInicio
            AND (AFJ.afj_dataFim IS NULL OR data_aula <= AFJ.afj_dataFim)
        LEFT JOIN {{ ref('raw_gestao_escolar__tipo_justificativa_falta') }} TJF
            ON TJF.tjf_id = AFJ.tjf_id
            AND TJF.tjf_situacao != 3
        LEFT JOIN {{ source('brutos_core_sso_staging', 'SYS_DiaNaoUtil') }} DNU
            ON (
                SQ_TA.data_aula = CAST(DNU.dnu_data AS DATE)
                OR
                (DNU.dnu_recorrencia = '1'
                    AND EXTRACT(MONTH FROM SQ_TA.data_aula) = EXTRACT(MONTH FROM CAST(DNU.dnu_data AS DATE))
                    AND EXTRACT(DAY FROM SQ_TA.data_aula) = EXTRACT(DAY FROM CAST(DNU.dnu_data AS DATE))
                )
            )
            AND DNU.dnu_situacao = '1'
        WHERE
            DNU.dnu_data IS NULL
            AND TJF.tjf_abonaFalta IS NULL
        GROUP BY
            MTU.alu_id,
            MTU.mtu_id,
            SQ_TA.id_tipo_calendario
    ) SQ_FALTAS
        ON NUM.alu_id = SQ_FALTAS.alu_id
        AND NUM.mtu_id = SQ_FALTAS.mtu_id
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

