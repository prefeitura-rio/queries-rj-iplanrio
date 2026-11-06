{{ config(cluster_by=["hash_id"]) }}

-- R-03 (lado dbt): o raw guarda uma linha por pessoa por NSU; a pessoa é única pelo
-- HASH_ID (SHA-256 dos 23 campos, calculado no parser). Os atributos são idênticos
-- para o mesmo HASH_ID, então basta o menor NSU.
select
    hash_id,
    cpf_cnpj,
    nome,
    opcao_simples_nacional,
    regime_apuracao_simples,
    regime_tributacao,
    nsu
from {{ nc2_raw("pessoas") }}
where hash_id is not null
qualify row_number() over (partition by hash_id order by nsu asc, ingerido_em desc) = 1
