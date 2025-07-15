{{
    config(
        alias="culturais",
        schema="brutos_equipamentos",
        materialized="table",
    )
}}

with
    source_data as (
        -- ETAPA 1: Seleciona e mapeia os dados da tabela de equipamentos culturais
        select
            numero as id_equipamento,
            equipamento,
            tipologia,
            endereco as rua,  -- Mapeia a coluna 'endereco' para o nome 'rua' para consistência
            bairro,
            cep,
            latitude,
            longitude
        from {{ source("brutos_equipamentos_staging","culturais_datario") }}
    ),

    standardized_data as (
        -- ETAPA 2: Limpeza e Padronização via SQL
        select
            *,
            trim(  -- 7. Remove espaços extras no início/fim da string final
                regexp_replace(  -- 6. Consolida múltiplos espaços em um único espaço
                    regexp_replace(  -- 5. Remove globalmente pontuações e símbolos indesejados
                        regexp_replace(  -- 4. Padroniza todas as variações de "Sem Número" para ' S/N '
                            regexp_replace(  -- 3. Converte uma lista de abreviações para os nomes completos
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    regexp_replace(
                                                        regexp_replace(
                                                            regexp_replace(
                                                                regexp_replace(
                                                                    regexp_replace(  -- 2. Adiciona conversões específicas solicitadas
                                                                        upper(rua),  -- 1. Converte toda a string para MAIÚSCULAS no início
                                                                        r'\bRUA 12\b',
                                                                        'RUA DOZE'  -- Converte "RUA 12" para "RUA DOZE"
                                                                    ),
                                                                    r'\bAV\.?\b',
                                                                    'AVENIDA'
                                                                ),
                                                                r'\bR\.?\b',
                                                                'RUA'
                                                            ),
                                                            r'\bEST\.?\b',
                                                            'ESTRADA'
                                                        ),
                                                        r'\bTRAV\.?\b',
                                                        'TRAVESSA'
                                                    ),
                                                    r'\bPÇA\.?\b',
                                                    'PRAÇA'
                                                ),
                                                r'\bALM\.?\b',
                                                'ALMIRANTE'
                                            ),
                                            r'\bGAL\.?\b',
                                            'GENERAL'
                                        ),
                                        r'\bGEN\.?\b',
                                        'GENERAL'
                                    ),
                                    r'\bPRES\.?\b',
                                    'PRESIDENTE'
                                ),
                                r'\bPROF\.?\b',
                                'PROFESSOR'
                            ),
                            r'\s*\b(S/N[º.°]?|S/\s?[º.°]?)\b\s*',
                            ' S/N '  -- 4
                        ),
                        r'[.,º°]|Nº|-',
                        ''  -- 5 (Hífen também removido aqui para simplificar)
                    ),
                    r'\s{2,}',
                    ' '  -- 6
                )
            ) as standardized_rua
        from source_data
    ),

    parsed_address as (
        -- ETAPA 3: Extração com a lógica corrigida para complemento
        select
            *,
            -- LOGRADOURO: Remove a *primeira* ocorrência de número/S/N e tudo que vem
            -- depois.
            trim(
                regexp_replace(
                    standardized_rua, r'\s+(?:\bS/N\b|\b\d[\d/]*[A-Z]?\b).*$', ''
                )
            ) as logradouro,

            -- NUMERO: Extrai a *primeira* ocorrência de um padrão de número ou 'S/N'.
            coalesce(
                regexp_extract(standardized_rua, r'(\bS/N\b|\b\d[\d/]*[A-Z]?\b)'), 'S/N'
            ) as numero,

            nullif(
                trim(
                    case
                        -- 1. Verifica se a rua REALMENTE contém um número ou S/N
                        when
                            regexp_contains(
                                standardized_rua, r'\bS/N\b|\b\d[\d/]*[A-Z]?\b'
                            )
                        -- 2. SE SIM, extrai o que vem depois do primeiro número
                        then
                            regexp_replace(
                                standardized_rua,
                                r'^.*?\s?(?:\bS/N\b|\b\d[\d/]*[A-Z]?\b)\s*',
                                ''
                            )
                        -- 3. SE NÃO, o complemento é vazio
                        else ''
                    end
                ),
                ''
            ) as complemento
        from standardized_data
    )

-- Seleção Final: Escolhe as colunas originais e as novas colunas tratadas.
select
    id_equipamento,
    upper({{ proper_br("tipologia") }}) as categoria,
    upper({{ proper_br("equipamento") }}) as nome,
    upper({{ proper_br("rua") }}) as rua_original,
    upper({{ proper_br("logradouro") }}) as logradouro,
    numero,
    upper({{ proper_br("complemento") }}) as complemento,
    upper({{ proper_br("bairro") }}) as bairro,
    cep,
    cast(latitude as float64) as latituasdasdde,
    cast(longitude as float64) as longitude,
    st_geogpoint(cast(longitude as float64), cast(latitude as float64)) as geometry
from parsed_address
