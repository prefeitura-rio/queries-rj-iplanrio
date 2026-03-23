{{
    config(
        alias='horario_trabalho',
        materialized="table",
        tags=["raw", "ergon", "horario_trabalho"],
        description="Horários possíveis para funcionários da administração direta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    id_horario,
    descricao,
    publicacao_diario_oficial,
    carga_horaria_mes,
    carga_horaria_semana,
    carga_horaria_dia,
    id_registro
FROM {{ ref('raw_recursos_humanos_ergon__horario_trabalho') }} AS t


