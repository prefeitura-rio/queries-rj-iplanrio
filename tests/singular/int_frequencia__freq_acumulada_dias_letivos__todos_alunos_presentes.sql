-- este teste checa o calculo da frequencia entre aquilo que é calculado no SGA (model
-- 'gestao_escolar_vw_bi_avlaliacao') contra o calculado no lake (model
-- 'mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos')

{{
    config(
        alias="int_frequencia__freq_acumulada_dias_letivos__todos_alunos_presentes",
        warn_if = ">0",
        error_if = ">5000"
    )
}}



with
    sga as (select * from {{ ref("gestao_escolar_vw_bi_avaliacao") }}),

    lake as (
        select *
        from {{ ref("int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos") }}
    ),

    joined as (

        select
            alu_id,
            sga.ano,
            sga.coc,
            sga.frequencia as freq_sga,
            lake.frequencia_percentual as freq_lake
        from sga
        left join
            lake
            on sga.alu_id = cast(lake.id_aluno as string)
            and sga.ano = cast(lake.ano_calendario as string)
            and sga.coc = cast(lake.id_tipo_calendario as string)
    ),

    final as (
        select *
        from joined
        where ano = '2026' and coc in ('1', '2') and freq_lake is null
    ),

    -- Correção temporária: remoção dos alunos EJA para tratamento;
    vw_bi_aluno as (
        select alu_id as id_aluno, Grupamento as grupamento
        from {{ ref('gestao_escolar__vw_bi_aluno') }} alu
    )

    select f.*
    from final f
    left join vw_bi_aluno bia
    on cast(f.alu_id as string) = cast(bia.id_aluno as string)
    where grupamento not like 'EJA%' and grupamento not like 'PROEJA%'


