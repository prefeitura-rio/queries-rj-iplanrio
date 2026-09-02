{{ config(
    alias='frequencia_acumulada_dias_letivos',
) }}

-- Nome frequencia acumulada dias letivos

WITH frequencia_acumulada_dias_letivos AS (
    -- 1ª Parte: Totais salvos nos COCs anteriores ao atual
    SELECT
        CAST(AAT.alu_id AS INT64) AS alu_id,
        CAST(CAP.tpc_id AS INT64) AS tpc_id,
        CAST(CAL.cal_ano AS INT64) AS ano_calendario,
        CAST(AAT.aat_numeroAulas AS INT64) AS numeroAulas,
        CAST(AAT.aat_numeroFaltas AS INT64) AS numeroFaltas
    FROM {{ ref('gestao_escolar__aluno_avaliacao_turma') }} AAT
    INNER JOIN {{ ref('gestao_escolar__aca_avaliacao') }} AVA
        ON AAT.fav_id = AVA.fav_id
        AND AAT.ava_id = AVA.ava_id
    INNER JOIN {{ ref('gestao_escolar__tur_turma') }} TUR
        ON AAT.tur_id = TUR.tur_id
        AND TUR.tur_situacao IN (1, 5)
    INNER JOIN {{ ref('gestao_escolar__calendario_anual') }} CAL
        ON TUR.cal_id = CAL.cal_id
    INNER JOIN {{ ref('gestao_escolar__calendario_periodo') }} CAP
        ON TUR.cal_id = CAP.cal_id
        AND AVA.tpc_id = CAP.tpc_id
        AND CAP.cap_dataFim < CURRENT_DATE() -- incluir filtro
    UNION ALL

    -- 2ª Parte: Totais das aulas já realizadas no COC atual (consome a view consolidada)
    SELECT
        CAST(id_aluno AS INT64) AS alu_id,
        CAST(id_tipo_calendario AS INT64) AS tpc_id,
        EXTRACT(YEAR FROM data_aula)      AS ano_calendario,
        SUM(numeroAulas) AS numeroAulas,
        SUM(falta) AS numeroFaltas     
    FROM {{ ref('mart_frequencia__vw_alunos_aulas') }}
    GROUP BY 
        id_aluno,
        id_tipo_calendario,
        EXTRACT(YEAR FROM data_aula)
)

SELECT
    alu_id AS id_aluno,
    tpc_id AS id_tipo_calendario,
    ano_calendario,
    numeroFaltas AS numero_faltas,
    numeroAulas AS numero_aulas,
    CASE
        WHEN numeroAulas = 0 THEN 0.00
        ELSE ROUND(100.00 - ((numeroFaltas * 1.00) / (numeroAulas * 1.00) * 100.00), 2)
    END AS frequencia_percentual

FROM frequencia_acumulada_dias_letivos