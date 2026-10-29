{{ config(cluster_by=["chave_acesso"]) }}

-- R-10: FISCALIZACAO (0 sem fiscalização, 1 em andamento, 2 concluída/encerrada). A
-- regra que leva o Oracle a gravar 2 em DFEN.NOTAS_NACIONAIS (RPE_FISCALIZACAO,
-- tarefas 27 e 28) ainda não está disponível (A confirmar em 04, R-10): por isso o
-- valor é REPLICADO da coluna FISCALIZACAO da réplica
-- brutos_nota_fiscal.NOTAS_NACIONAIS, e o modelo marca a regra como origem_replicada.
-- Nota sem linha na réplica ou com valor nulo vale 0, como COALESCE(fiscalizacao, 0)
-- <> 2 do R-10. Troque a origem aqui quando o corpo de RPE_FISCALIZACAO existir; as
-- exigíveis só leem a coluna fiscalizacao.
select
    n.chave_acesso,
    n.nsu,
    coalesce(cast(r.fiscalizacao as int64), 0) as fiscalizacao,
    'origem_replicada' as origem_fiscalizacao
from {{ ref("int_nc2__dedup") }} as n
left join
    {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }} as r
    on r.chave_acesso = n.chave_acesso
