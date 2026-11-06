-- R-06 e R-07: a cascata de CENARIO_EXIGIBILIDADE sobre as seeds, com a correção
-- catalogada B-04. 4 e 5 vêm antes da habilitação; -1 só no ISS próprio sem
-- INSCRICAO_NOTANACIONAL = 1; 1 (IF ativa), 2 (pedágio e grupo 22) e 3 (Simples
-- regime 1) só no ISS próprio; 6 (suspensão) em ISS próprio e retido; o resto,
-- inclusive toda retenção, é 0. A nota retida de contribuinte sem habilitação dá 0
-- (B-04), e não -1 (que é o que o PL/SQL grava).
with
    esperado as (
        select
            'A01_NAO_RETIDA' as chave_acesso,
            0 as cenario,
            'ISS próprio, responsável habilitado' as caso
        union all
        select
            'A02_RETIDA_ABRIL',
            4,
            'competência do serviço antes de InicioPlanoB (Plano C), mesmo retida em maio'
        union all
        select
            'A03_RETIDA_TOMADOR',
            0,
            'B-04: retida de contribuinte sem habilitação é 0, não -1'
        union all
        select
            'A04_RETIDA_INTERMEDIARIO',
            0,
            'B-04: retida pelo intermediário sem habilitação é 0, não -1'
        union all
        select 'A06_SERVICO_22', 2, 'pedágio habilitado no grupo de serviço 22'
        union all
        select
            'H01_SIMPLES_REGIME_2',
            0,
            'Simples com regime 2 (federais pelo SN, ISS fora) é cenário 0'
        union all
        select 'H02_SIMPLES_REGIME_1', 3, 'Simples apurado pelo DAS (regime 1)'
        union all
        select
            'H03_SIMPLES_SEM_REGIME',
            0,
            'Simples com regime nulo cai em 0 (segurança contra NULL)'
        union all
        select
            'H04_INSTITUICAO_FINANCEIRA',
            1,
            'instituição financeira ativa com ISS próprio'
        union all
        select 'H05_IF_INATIVA', 0, 'DESIF inativa (STATUS 1) não é cenário 1'
        union all
        select 'H06_IF_E_SIMPLES', 1, 'IF e Simples: vale 1 (1 antes de 3)'
        union all
        select 'H08_PEDAGIO_OUTRO_GRUPO', 0, 'pedágio fora do grupo 22'
        union all
        select 'H09_NAO_HABILITADO', -1, 'ISS próprio sem habilitação aguarda (-1)'
        union all
        select 'H10_PLANO_C_NAO_HAB', 4, 'B-04: 4 vem antes da habilitação'
        union all
        select 'H11_SEM_ISS_NAO_HAB', 5, 'B-04: 5 vem antes da habilitação'
        union all
        select 'H12_ISSQN_2', 5, 'ISSQN diferente de 1'
        union all
        select 'H13_ISSQN_NULO', 5, 'ISSQN nulo conta como diferente de 1'
        union all
        select 'H14_FORA_DO_RIO', 5, 'incidência diferente de 3304557'
        union all
        select 'H15_SUSPENSAO_1', 6, 'suspensão judicial'
        union all
        select 'H16_SUSPENSAO_2', 6, 'suspensão administrativa'
        union all
        select 'H17_RETIDA_SUSPENSA', 6, 'suspensão vale também no ISS retido'
        union all
        select 'H18_RETIDA_HABILITADA', 0, 'ISS retido é 0'
        union all
        select 'H19_IF_RETIDA', 0, 'IF só é cenário 1 no ISS próprio'
        union all
        select 'H20_DUAS_INSCRICOES', 0, 'basta uma inscrição habilitada'
        union all
        select 'H21_SIMPLES_RETIDA', 0, 'Simples só é cenário 3 no ISS próprio'
        union all
        select
            'H22_HABILITADO_PRESO_NO_ORACLE',
            0,
            'R-07: habilitado hoje, sem janela de 7 dias: promovida a 0'
    )

select
    e.chave_acesso,
    e.caso,
    cast(e.cenario as string) as esperado,
    cast(c.cenario_exigibilidade as string) as obtido
from esperado as e
left join {{ ref("int_nc2__cenario") }} as c on c.chave_acesso = e.chave_acesso
where c.cenario_exigibilidade is distinct from e.cenario
union all
select
    chave_acesso,
    'responsável_habilitado: só as notas com INSCRICAO_NOTANACIONAL = 1 no responsável',
    'true',
    cast(responsavel_habilitado as string)
from {{ ref("int_nc2__cenario") }}
where
    (chave_acesso = 'H09_NAO_HABILITADO' and responsavel_habilitado)
    or (chave_acesso = 'H22_HABILITADO_PRESO_NO_ORACLE' and not responsavel_habilitado)
    or (chave_acesso = 'H20_DUAS_INSCRICOES' and not responsavel_habilitado)
union all
select
    'todas',
    'nenhuma nota pode ficar sem cenário (a cascata sempre decide)',
    '0',
    cast(count(*) as string)
from {{ ref("int_nc2__cenario") }}
where cenario_exigibilidade is null
having count(*) > 0
