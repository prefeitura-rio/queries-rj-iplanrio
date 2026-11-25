{{ config(materialized="table", contract={"enforced": true}) }}

-- Resumo da paridade F1: uma linha por regra (R-01..R-11) e classe (igual,
-- correcao_catalogada:<B-xx>, divergente). Contrato lido pelo flow
-- rj_smfp__nota_carioca_paridade_f1 (não mude): regra STRING, classe STRING,
-- quantidade INT64, corte_nsu INT64, gerado_em TIMESTAMP. Os comparados de uma regra
-- são a soma de quantidade; o flow reprova se divergente > 0 ou se comparados = 0 em
-- alguma regra.
select
    regra,
    classe,
    count(*) as quantidade,
    (select corte_nsu from {{ ref("paridade_nc2__corte") }}) as corte_nsu,
    current_timestamp() as gerado_em
from
    (
        select regra, classe
        from {{ ref("paridade_nc2__r01") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r02") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r03") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r04") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r05") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r06") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r07") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r08") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r09") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r10") }}
        union all
        select regra, classe
        from {{ ref("paridade_nc2__r11") }}
    )
group by regra, classe
