{{
    config(
        partition_by={
            "field": "data_compmunicipio",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["chave_acesso"],
    )
}}

-- R-01: competência municipal (DATA_COMPMUNICIPIO / DATA_COMPETENCIA_MUNICIPIO) =
-- primeiro dia do mês de DPS.DATA_COMPETENCIA, mais 1 mês quando o ISS é retido
-- (tpRetISSQN 2 = tomador, 3 = intermediário) (Dps.vb:1461-1481,
-- NotasNacionais.vb:3783-3864). tpRetISSQN nulo mantém o mês (no XSD válido é sempre
-- 1, 2 ou 3). A elegibilidade ao Plano B continua usando a competência do serviço
-- (data_competencia_servico), não esta.
select
    *,
    case
        when tipo_retencao_issqn in (2, 3)
        then date_add(date_trunc(data_competencia_servico, month), interval 1 month)
        else date_trunc(data_competencia_servico, month)
    end as data_compmunicipio
from {{ ref("int_nc2__dedup") }}
