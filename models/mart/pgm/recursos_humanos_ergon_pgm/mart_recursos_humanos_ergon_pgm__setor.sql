{{
    config(
        alias='setor',
        materialized="table",
        tags=["mart", "ergon", "setor"],
        description="Tabela que contém os registros do histórico da estrutura da PGM com todos os seus setores e em todas as suas versões."
    )
}}

SELECT
    id_setor,
    nome,
    nome_completo,
    sigla,
    data_inicio,
    data_fim,
    id_setor_pai,
    id_empresa,
    id_empresa_prevrio,
    id_secretaria,
    case upper(extinto)
        when 'S' then 'Sim'
        when 'N' then 'Não'
        else 'Não Informado'
    end as extinto,
    updated_at as data_atualizacao
FROM {{ ref('raw_recursos_humanos_ergon__setor')}}
where id_secretaria = '2200'


