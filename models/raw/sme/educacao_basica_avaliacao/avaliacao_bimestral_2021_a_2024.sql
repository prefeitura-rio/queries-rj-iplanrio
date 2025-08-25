WITH d2021_a_2024 AS
(
  WITH d2021 as (
SELECT
  SPLIT(tur_id, ".")[0] as id_turma,
  SPLIT(alu_id, ".")[0] as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(bimestre_avaliacao as INT64) as bimestre,
  SPLIT(inep, ".")[0] as codigo_inep_escola,
  SAFE_CAST(grupamento as STRING) as aluno_grupamento,
  SAFE_CAST(NULL AS STRING) AS aluno_atendimento_especializado,
  CASE
    WHEN nm_disciplina_sigla = "LP" THEN 1
    WHEN nm_disciplina_sigla = "MT" THEN 2
    ELSE NULL
    END as codigo_disciplina,
  CASE
    WHEN nm_disciplina_sigla IS NOT NULL THEN SAFE_CAST(nm_disciplina_sigla as STRING)
    ELSE NULL
    END as sigla_disciplina,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_previsto as FLOAT64) AS INT64) AS BOOL) as indicador_previsto_avaliacao,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_avaliado as FLOAT64) AS INT64) AS BOOL) as indicador_avaliado,
  NULL AS numero_questoes_total,
  NULL AS numero_questoes_respondidas,
  NULL AS numero_questoes_corretas,
  SAFE_CAST(cat_desempenho AS STRING) as indice_acerto,
  SAFE_CAST(vl_perc_acertos AS FLOAT64) as taxa_acerto,
  NULL AS proficiencia_valor,
  SAFE_CAST(NULL AS FLOAT64) AS proficiencia_erro,
  NULL AS proficiencia_escala_350_valor,
  SAFE_CAST(NULL AS STRING) AS padrao_desempenho,
  SAFE_CAST(NULL AS STRING) AS nivel_intervencao,
  SAFE_CAST(NULL AS STRING) AS habilidades_avaliadas_codigo,
  SAFE_CAST(NULL AS STRING) AS habilidades_avaliadas_nome,
  SAFE_CAST(NULL AS STRING) AS habilidades_avaliadas_acertos,
  SAFE_CAST(NULL AS STRING) AS habilidades_avaliadas_total
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2021`
),

d2022 as (
SELECT
  SPLIT(id_turma, ".")[0] as id_turma,
  SPLIT(id_aluno, ".")[0] as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(SPLIT(bimestre_avaliacao, "º")[0] as INT64) as bimestre,
  SPLIT(id_inep, ".")[0] as codigo_inep_escola,
  SAFE_CAST(grupamento as STRING) as aluno_grupamento,
  SAFE_CAST(dc_atendimento_especializado as STRING) AS aluno_atendimento_especializado,
  CASE
    WHEN cd_disciplina IS NOT NULL THEN SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64)
    WHEN nm_disciplina = "LÍNGUA PORTUGUESA" THEN 1
    WHEN nm_disciplina = "MATEMÁTICA" THEN 2
    ELSE NULL
    END as codigo_disciplina,
  CASE
    WHEN nm_disciplina = "LÍNGUA PORTUGUESA" OR SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64) = 1 THEN "LP"
    WHEN nm_disciplina = "MATEMÁTICA" OR SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64) = 2 THEN "MT"
    ELSE NULL
    END as sigla_disciplina,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_previsto as FLOAT64) AS INT64) AS BOOL) as indicador_previsto_avaliacao,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_avaliado as FLOAT64) AS INT64) AS BOOL) as indicador_avaliado,
  SAFE_CAST(SAFE_CAST(nu_total as FLOAT64) AS INT64) AS numero_questoes_total,
  NULL AS numero_questoes_respondidas,
  SAFE_CAST(SAFE_CAST(nu_acerto as FLOAT64) AS INT64) AS numero_questoes_corretas,
  SAFE_CAST(dc_acerto AS STRING) as indice_acerto,
  SAFE_CAST(tx_acerto AS FLOAT64) as taxa_acerto,
  SAFE_CAST(SAFE_CAST(vl_proficiencia as FLOAT64) AS INT64) as proficiencia_valor,
  SAFE_CAST(vl_proficiencia_erro as FLOAT64) as proficiencia_erro,
  SAFE_CAST(SAFE_CAST(REPLACE(vl_proficiencia_350, ",", ".") as FLOAT64) AS INT64) as proficiencia_escala_350_valor,
  SAFE_CAST(dc_padrao_desempenho AS STRING) as padrao_desempenho,
  SAFE_CAST(dc_intervencao AS STRING) as nivel_intervencao,
  SAFE_CAST(cd_habilidade AS STRING) as habilidades_avaliadas_codigo,
  SAFE_CAST(nm_habilidade AS STRING) as habilidades_avaliadas_nome,
  SAFE_CAST(dc_habilidade_acerto AS STRING) as habilidades_avaliadas_acertos,
  SAFE_CAST(dc_habilidade_total AS STRING) as habilidades_avaliadas_total
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2022`
),

d2023 as (
SELECT
  SPLIT(id_turma, ".")[0] as id_turma,
  SPLIT(id_aluno, ".")[0] as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(SPLIT(bimestre_avaliacao, "º")[0] as INT64) as bimestre,
  SPLIT(id_inep, ".")[0] as codigo_inep_escola,
  SAFE_CAST(grupamento as STRING) as aluno_grupamento,
  SAFE_CAST(dc_atendimento_especializado as STRING) AS aluno_atendimento_especializado,
  CASE
    WHEN cd_disciplina IS NOT NULL THEN SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64)
    WHEN nm_disciplina = "Língua Portuguesa" THEN 1
    WHEN nm_disciplina = "Matemática" THEN 2
    ELSE NULL
    END as codigo_disciplina,
  CASE
    WHEN nm_disciplina = "Língua Portuguesa" OR SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64) = 1 THEN "LP"
    WHEN nm_disciplina = "Matemática" OR SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64) = 2 THEN "MT"
    ELSE NULL
    END as sigla_disciplina,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_previsto as FLOAT64) AS INT64) AS BOOL) as indicador_previsto_avaliacao,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_avaliado as FLOAT64) AS INT64) AS BOOL) as indicador_avaliado,
  SAFE_CAST(SAFE_CAST(nu_total as FLOAT64) AS INT64) AS numero_questoes_total,
  SAFE_CAST(SAFE_CAST(nu_resposta as FLOAT64) AS INT64) AS numero_questoes_respondidas,
  SAFE_CAST(SAFE_CAST(nu_acerto as FLOAT64) AS INT64) AS numero_questoes_corretas,
  SAFE_CAST(dc_acerto AS STRING) as indice_acerto,
  SAFE_CAST(tx_acerto AS FLOAT64) as taxa_acerto,
  SAFE_CAST(SAFE_CAST(vl_proficiencia as FLOAT64) AS INT64) as proficiencia_valor,
  SAFE_CAST(vl_proficiencia_erro as FLOAT64) as proficiencia_erro,
  SAFE_CAST(SAFE_CAST(vl_proficiencia_350 as FLOAT64) AS INT64) as proficiencia_escala_350_valor,
  SAFE_CAST(dc_padrao_desempenho AS STRING) as padrao_desempenho,
  SAFE_CAST(dc_intervencao AS STRING) as nivel_intervencao,
  SAFE_CAST(cd_habilidade AS STRING) as habilidades_avaliadas_codigo,
  SAFE_CAST(nm_habilidade AS STRING) as habilidades_avaliadas_nome,
  SAFE_CAST(dc_habilidade_acerto AS STRING) as habilidades_avaliadas_acertos,
  SAFE_CAST(dc_habilidade_total AS STRING) as habilidades_avaliadas_total
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2023`
),

d2024 as (
SELECT
  SPLIT(id_turma, ".")[0] as id_turma,
  SPLIT(id_aluno, ".")[0] as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(SPLIT(bimestre_avaliacao, "º")[0] as INT64) as bimestre,
  SPLIT(id_inep, ".")[0] as codigo_inep_escola,
  SAFE_CAST(grupamento as STRING) as aluno_grupamento,
  SAFE_CAST(dc_atendimento_especializado as STRING) AS aluno_atendimento_especializado,
  CASE
    WHEN cd_disciplina IS NOT NULL THEN SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64)
    WHEN nm_disciplina = "Língua Portuguesa" THEN 1
    WHEN nm_disciplina = "Matemática" THEN 2
    ELSE NULL
    END as codigo_disciplina,
  CASE
    WHEN nm_disciplina = "Língua Portuguesa" OR SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64) = 1 THEN "LP"
    WHEN nm_disciplina = "Matemática" OR SAFE_CAST(SPLIT(cd_disciplina, ".")[0] AS INT64) = 2 THEN "MT"
    ELSE NULL
    END as sigla_disciplina,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_previsto as FLOAT64) AS INT64) AS BOOL) as indicador_previsto_avaliacao,
  SAFE_CAST(SAFE_CAST(SAFE_CAST(fl_avaliado as FLOAT64) AS INT64) AS BOOL) as indicador_avaliado,
  SAFE_CAST(SAFE_CAST(nu_total as FLOAT64) AS INT64) AS numero_questoes_total,
  SAFE_CAST(SAFE_CAST(nu_resposta as FLOAT64) AS INT64) AS numero_questoes_respondidas,
  SAFE_CAST(SAFE_CAST(nu_acerto as FLOAT64) AS INT64) AS numero_questoes_corretas,
  SAFE_CAST(dc_acerto AS STRING) as indice_acerto,
  SAFE_CAST(tx_acerto AS FLOAT64) as taxa_acerto,
  SAFE_CAST(SAFE_CAST(vl_proficiencia as FLOAT64) AS INT64) as proficiencia_valor,
  SAFE_CAST(vl_proficiencia_erro as FLOAT64) as proficiencia_erro,
  SAFE_CAST(SAFE_CAST(vl_proficiencia_350 as FLOAT64) AS INT64) as proficiencia_escala_350_valor,
  SAFE_CAST(dc_padrao_desempenho AS STRING) as padrao_desempenho,
  SAFE_CAST(NULL AS STRING) as nivel_intervencao,
  SAFE_CAST(cd_habilidade AS STRING) as habilidades_avaliadas_codigo,
  SAFE_CAST(nm_habilidade AS STRING) as habilidades_avaliadas_nome,
  SAFE_CAST(dc_habilidade_acerto AS STRING) as habilidades_avaliadas_acertos,
  SAFE_CAST(dc_habilidade_total AS STRING) as habilidades_avaliadas_total
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2024`
)

SELECT * FROM d2021

UNION ALL

SELECT * FROM d2022

UNION ALL

SELECT * FROM d2023

UNION ALL

SELECT * FROM d2024
)

SELECT
  TRIM(id_aluno) as id_aluno_original,
  SUBSTR(SHA256(
        CONCAT(
            '{{ var("HASH_SEED") }}',
            TRIM(id_aluno)
        )
    ), 2,17) as  id_aluno,
  * EXCEPT(id_aluno)
FROM d2021_a_2024
WHERE id_aluno IS NOT NULL OR taxa_acerto IS NOT NULL
ORDER BY ano, bimestre, id_aluno, sigla_disciplina
