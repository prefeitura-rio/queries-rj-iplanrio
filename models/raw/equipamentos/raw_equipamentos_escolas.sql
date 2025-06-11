
{{
    config(
        alias="escolas",
        schema="brutos_equipamentos",
        materialized="table",
    )
}}

with
    source_data as (
        select
            cre,
            designacao,
            tipo,
            denominacao,
            cep,
            rua,
            bairro,
            latitude,
            longitude,
        from {{ source("brutos_equipamentos_staging", "escolas") }}
    ),

    standardized_data as (
        -- ETAPA 1: Limpeza e Padronização Definitiva. (Esta CTE está perfeita, sem alterações)
        select
            *,
            trim( -- 6. Remove espaços no início/fim
                regexp_replace( -- 5. Consolida múltiplos espaços em um único espaço
                    regexp_replace( -- 4. Remove pontuações e símbolos indesejados de forma geral
                        regexp_replace( -- 3. Padroniza todas as variações de "Sem Número" para ' S/N '
                            -- 2. Converte uma lista extensa de abreviações para os nomes completos
                            regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                                upper(
                                  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                                    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(rua, 
                                  "31 de março", "Trinta e Um de março"),
                                  "28 De Setembro,","vinte e oito De Setembro,"),
                                  "13 De Julho,","Treze De Julho,"),
                                  "Rua 222,","Rua Dois Dois Dois,"),
                                  "15 De Novembro,","Quinze de novembro,"),
                                  "Rua 2, Esq. Rua 17","Rua dois, Esq. Rua dezessete,"),
                                  "Rua 1,","Rua Um,"),
                                  "Rua 5 PAA 10809/PAL42839 - acesso pela Rua 4","Rua Cinco PAA 10809/PAL42839 - acesso pela Rua Quatro"),
                                  "Rua 23,","Rua Vinte e Tres,"),
                                  "Rua 54,","Rua Cinquenta e Quatro,"),
                                  "Rua 15,","Rua Quinze"),
                                  "Rua 7,","Rua Sete"),
                                  "Rua 27,","Rua Vinte e Sete"),
                                  "Rua 9,","Rua Nove")
                                  
                                ), -- 1. Converte toda a string para MAIÚSCULAS
                                r'\bAV\.?\b', 'AVENIDA'),
                                r'\bR\.?\b', 'RUA'),
                                r'\bESTR\.?\b', 'ESTRADA'),
                                r'\bTRAV\.?\b', 'TRAVESSA'),
                                r'\bPÇA\.?\b', 'PRAÇA'),
                                r'\bALM\.?\b', 'ALMIRANTE'),
                                r'\bGAL\.?\b', 'GENERAL'),
                                r'\bGEN\.?\b', 'GENERAL'),
                                r'\bPRES\.?\b', 'PRESIDENTE'),
                                r'\bPROF\.?\b', 'PROFESSOR'),
                            r'\s*\b(S/N[º.°]?|S/\s?[º.°]?)\b\s*', ' S/N ' -- 3
                        ),
                        r'[.,º°]|Nº', '' -- 4
                    ),
                r'\s{2,}', ' ' -- 5
            )) as standardized_rua
        from source_data
    ),

    parsed_address as (
        -- ETAPA 2: Extração com a nova estratégia "Primeira Ocorrência"
        select
            *,
            -- LOGRADOURO: Remove a *primeira* ocorrência de número/S/N e tudo que vem depois.
            trim(regexp_replace(
                standardized_rua,
                r'\s+(?:\bS/N\b|\b\d[\d/-]*[A-Z]?\b).*$', -- O '.*$' no final garante que o complemento também seja removido
                ''
            )) as logradouro,

            -- NUMERO: Extrai a *primeira* ocorrência de um padrão de número ou 'S/N'.
            coalesce(
                regexp_extract(standardized_rua, r'(\bS/N\b|\b\d[\d/-]*[A-Z]?\b)'),
                'S/N'
            ) as numero,

            -- COMPLEMENTO: Remove tudo até a *primeira* ocorrência do número, deixando o resto.
            nullif(
                trim(
                  REGEXP_REPLACE(
                                        case
                        when regexp_contains(standardized_rua, r'\bS/N\b|\b\d[\d/-]*[A-Z]?\b')
                        then regexp_replace(standardized_rua, r'^.*?\s?(?:\bS/N\b|\b\d[\d/-]*[A-Z]?\b)\s*', '')
                        else ''
                    end
                    , "-", "")
                ),
                ''
            ) as complemento
        from standardized_data
    )

-- Seleção Final: Escolhe as colunas originais e as novas colunas tratadas.
select
    cre,
    designacao,
    {{proper_br("tipo")}} as categoria,
    {{proper_br("denominacao")}} as nome,
    {{proper_br("rua")}} as rua,
    {{ proper_br("logradouro") }} as logradouro,
    numero,
    {{ proper_br("complemento") }} as complemento,
    {{ proper_br("bairro") }} as bairro,
    cep,
    CAST(latitude AS FLOAT64) AS latituasdasdde,
    CAST(longitude AS FLOAT64) AS longitude,
    st_geogpoint(CAST(longitude AS FLOAT64), CAST(latitude AS FLOAT64)) as geometry,
from parsed_address