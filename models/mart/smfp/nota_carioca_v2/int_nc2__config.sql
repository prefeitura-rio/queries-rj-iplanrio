{#
    Parâmetros de NFSE_OWNER.CONFIGURACOES (sessão NotaNacional) que mudam o resultado das regras do v2, em texto DD/MM/AAAA como o
    Oracle os guarda. Sem a chave vale o default do código (Date.MaxValue):
      IniExtDocsFiscais  R-04, corte de competência na extração
      InicioPlanoB       R-06, início do Plano B
#}
with
    parametros as (
        select chave, safe.parse_date('%d/%m/%Y', valor) as valor_data
        from {{ nc2_fonte("nota_carioca", "CONFIGURACOES") }}
        where sessao = 'NotaNacional'
    )

select
    coalesce(
        max(if(chave = 'IniExtDocsFiscais', valor_data, null)), date '9999-12-31'
    ) as ini_ext_docs_fiscais,
    coalesce(
        max(if(chave = 'InicioPlanoB', valor_data, null)), date '9999-12-31'
    ) as inicio_plano_b
from parametros
