{{
    config(
        alias='forma_provimento',
        materialized="table",
        tags=["raw", "forma_provimento", "ergon"],
        description="Tabela que contém as formas de provimentos possíveis tanto na administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    id_forma_provimento,
    nome,
    case inativo
        when 'S' then true
        when 'N' then false
        else null
    end as inativo,
    case upper(primeiro_provimento)
        when 'S' then true
        when 'N' then false
        else null
    end as primeiro_provimento,
    case upper(ativo)
        when 'S' then true
        when 'N' then false
        else null
    end as ativo
FROM {{ ref('raw_recursos_humanos_ergon__forma_provimento') }} AS t