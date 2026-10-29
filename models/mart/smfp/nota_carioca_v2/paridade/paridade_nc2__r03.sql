-- R-03: HASH_ID (SHA-256 de 23 campos, calculado no parser) por pessoa, entre
-- DFEN.PESSOAS_NACIONAIS (RAW(32) em base64 na réplica, convertido para hex
-- maiúsculo) e int_nc2__dedup_pessoas, mais os atributos da pessoa. A linha sentinela
-- do Oracle (zero-GUID, ADR-041) e pessoas sem HASH_ID ficam de fora: não nascem de
-- XML. O Oracle não guarda o NSU da pessoa, então o corte vale só para a nuvem (NSU
-- <= corte).
with
    v2 as (
        select
            hash_id as chave,
            cpf_cnpj,
            nome,
            cast(opcao_simples_nacional as numeric) as opcao_simples_nacional,
            cast(regime_apuracao_simples as numeric) as regime_apuracao_simples,
            cast(regime_tributacao as numeric) as regime_tributacao
        from {{ ref("int_nc2__dedup_pessoas") }}
        where nsu <= {{ nc2_corte() }}
    ),

    oracle as (
        select
            upper(to_hex(from_base64(hash_id))) as chave,
            cpf_cnpj,
            nome,
            opcao_simples_nacional,
            regime_apuracao_simples,
            regime_tributacao
        from {{ nc2_fonte("nota_carioca", "PESSOAS_NACIONAIS") }}
        where hash_id is not null and pessoa_nacional <> 'AAAAAAAAAAAAAAAAAAAAAA=='
    ),

    comparado as (
        select
            coalesce(v.chave, o.chave) as chave,
            v.chave is not null as no_v2,
            o.chave is not null as no_oracle,
            (
                v.cpf_cnpj is not distinct from o.cpf_cnpj
                and v.nome is not distinct from o.nome
                and v.opcao_simples_nacional
                is not distinct from o.opcao_simples_nacional
                and v.regime_apuracao_simples
                is not distinct from o.regime_apuracao_simples
                and v.regime_tributacao is not distinct from o.regime_tributacao
            ) as colunas_iguais
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
    )

select
    'R-03' as regra,
    chave,
    if(no_v2 and no_oracle and colunas_iguais, 'igual', 'divergente') as classe,
    case
        when not no_oracle
        then 'so_v2'
        when not no_v2
        then 'so_oracle'
        when not colunas_iguais
        then 'colunas_diferentes'
    end as detalhe
from comparado
