-- R-11: as exigíveis do v2 (status 0, fiscalização <> 2, cenário 0, pessoas
-- resolvidas) e o resumo por dimensão.
with
    esperado as (
        select chave_acesso
        from
            unnest(
                [
                    'A01_NAO_RETIDA',
                    'A03_RETIDA_TOMADOR',
                    'A04_RETIDA_INTERMEDIARIO',
                    'A05_SEM_TOMADOR',
                    'A07_DEDUCAO',
                    'A08_DEDUCAO_NULA',
                    'B01_DUPLICATA_NSU',
                    'B02_REPROCESSADA',
                    'C05_BLOQ_DESBLOQ',
                    'C08_EMPATE',
                    'C09_IRRELEVANTE',
                    'C11_CANCELA_DESBLOQ',
                    'D02_SUBSTITUTA',
                    'D04_SUBSTITUTA',
                    'E03_CADEIA_C',
                    'F02_SUBSTITUTA_DE_F01',
                    'F04_SUBSTITUTA_DE_F03',
                    'G01_ALVO_INEXISTENTE',
                    'H01_SIMPLES_REGIME_2',
                    'H03_SIMPLES_SEM_REGIME',
                    'H05_IF_INATIVA',
                    'H08_PEDAGIO_OUTRO_GRUPO',
                    'H18_RETIDA_HABILITADA',
                    'H19_IF_RETIDA',
                    'H20_DUAS_INSCRICOES',
                    'H21_SIMPLES_RETIDA',
                    'H22_HABILITADO_PRESO_NO_ORACLE',
                    'I02_FISCALIZACAO_1',
                    'I03_FISCALIZACAO_NULA',
                    'K01_ACIMA_DO_CORTE',
                    'M02_SUBSTITUTA_ALEM_DO_CORTE'
                ]
            ) as chave_acesso
    ),

    obtido as (
        select chave_acesso from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
    ),

    grupo_principal as (
        select *
        from {{ ref("mvt_notas_nacionais_exigiveis") }}
        where
            cpf_cnpj_responsavel = '98000001000101'
            and data_competencia_municipio = '2026-06-01T00:00:00'
            and tipo_retencao = 1
            and opcao_simples_nacional = 1
            and retencao = 0
    )

select
    coalesce(e.chave_acesso, o.chave_acesso) as chave_acesso,
    if(
        o.chave_acesso is null,
        'deveria ser exigível e não está',
        'está nas exigíveis e não deveria'
    ) as falha,
    cast(null as string) as esperado,
    cast(null as string) as obtido
from esperado as e
full outer join obtido as o on o.chave_acesso = e.chave_acesso
where e.chave_acesso is null or o.chave_acesso is null
union all
select
    'resumo',
    'a soma de quantidade_notas do resumo é o número de linhas do detalhe',
    cast((select count(*) from obtido) as string),
    cast(
        (
            select sum(quantidade_notas) from {{ ref("mvt_notas_nacionais_exigiveis") }}
        ) as string
    )
from (select 1)
where
    (select count(*) from obtido) is distinct from (
        select sum(quantidade_notas) from {{ ref("mvt_notas_nacionais_exigiveis") }}
    )
union all
select
    'resumo',
    'o grupo do prestador comum em 2026-06: 19 notas, 18 com dedução, soma exata das deduções, do ISS e da base',
    '19|18|10.50|1895.00|94750.00|95000.00',
    concat(
        cast(quantidade_notas as string),
        '|',
        cast(cnt_valor_ded_red as string),
        '|',
        cast(total_valor_ded_red as string),
        '|',
        cast(total_valor_issqn as string),
        '|',
        cast(total_valor_base_calculo as string),
        '|',
        cast(total_valor_servico as string)
    )
from grupo_principal
where
    quantidade_notas <> 19
    or cnt_valor_ded_red <> 18
    or total_valor_ded_red <> 10.50
    or total_valor_issqn <> 1895.00
    or total_valor_base_calculo <> 94750.00
    or total_valor_servico <> 95000.00
union all
select 'resumo', 'o grupo do prestador comum em 2026-06 existe', '1', '0'
from (select 1)
where not exists (select 1 from grupo_principal)
union all
select
    chave_acesso,
    'a sentinela do tomador ausente sai com CPF/CNPJ de 14 zeros e nome nulo',
    '00000000000000|',
    concat(cpf_cnpj_contraparte, '|', coalesce(nome_contraparte, ''))
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where
    chave_acesso = 'A05_SEM_TOMADOR'
    and (cpf_cnpj_contraparte <> '00000000000000' or nome_contraparte is not null)
union all
select
    chave_acesso,
    'a nota B01 sai com o valor da ocorrência de menor NSU e a B02 com o do reprocessamento',
    '100.00',
    cast(valor_issqn as string)
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where chave_acesso in ('B01_DUPLICATA_NSU', 'B02_REPROCESSADA') and valor_issqn <> 100
union all
select
    chave_acesso,
    'retenção, tipo de retenção e Simples do prestador na saída',
    '1|2|3',
    concat(
        cast(retencao as string),
        '|',
        cast(tipo_retencao as string),
        '|',
        cast(opcao_simples_nacional as string)
    )
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where
    chave_acesso = 'H21_SIMPLES_RETIDA'
    and (retencao <> 1 or tipo_retencao <> 2 or opcao_simples_nacional <> 3)
union all
select
    chave_acesso,
    'R-01: retida em junho vai para julho no texto da saída exigível',
    '2026-07-01T00:00:00',
    data_competencia_municipio
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where
    chave_acesso = 'A03_RETIDA_TOMADOR'
    and data_competencia_municipio <> '2026-07-01T00:00:00'
union all
select
    'C05_BLOQ_DESBLOQ',
    'R-08: bloqueio seguido de desbloqueio (status 0) entra nas exigíveis',
    '1',
    cast(count(*) as string)
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where chave_acesso = 'C05_BLOQ_DESBLOQ'
having count(*) <> 1
