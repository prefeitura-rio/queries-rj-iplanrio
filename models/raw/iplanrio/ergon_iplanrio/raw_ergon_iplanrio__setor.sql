{{
    config(
        alias='setor',
        materialized="table",
        tags=["raw", "ergon", "setor"],
        description="Tabela que contém os registros dos setores da IplanRio disponíveis no Ergon."
    )
}}

/*
    Filtrando apenas os registros dos setores da IplanRio disponíveis no Ergon.
*/
SELECT
    id_setor,
    id_setor_pai,
    data_inicio,
    data_fim,
    nome,
    nome_completo,
    sigla,
    id_empresa,
    id_empresa_prevrio,
    id_secretaria,
    extinto,
    updated_at
FROM {{ ref("raw_recursos_humanos_ergon__setor") }}
where id_empresa = '18'