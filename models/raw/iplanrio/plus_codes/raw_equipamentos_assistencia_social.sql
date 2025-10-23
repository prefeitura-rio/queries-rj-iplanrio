{{
    config(
        alias="equipamentos_assistencia_social",
        schema="plus_codes",
        materialized="table",
    )
}}

with
    base_geometria as (
        select * from {{ source("smas_equipamentos", "poligonos_rmi") }}
    ),
    
    base_enderecos_raw as (
        select * from {{ source("smas_equipamentos", "poligonos_rmi_enderecos_2") }}
    ),
    
    base_enderecos as (
        select
            *,
            -- Tratar latitude: converter para string, fazer RPAD com 0 para 8 caracteres, converter para int e dividir por 10^6
            cast(cast(rpad(cast(latitude as string), 8, '0') as int64) as float64) / 100000 as latitude_tratada,
            -- Tratar longitude: converter para string, fazer RPAD com 0 para 8 caracteres, converter para int e dividir por 10^6
            cast(cast(rpad(cast(longitude as string), 8, '0') as int64) as float64) / 100000 as longitude_tratada
        from base_enderecos_raw
    ),
    
    tb as (
        select
            -- Usar latitude/longitude dos endereços quando disponível, senão usar centroide
            coalesce(e.latitude_tratada, st_y(st_centroid(t.geometry))) as lat_final,
            coalesce(e.longitude_tratada, st_x(st_centroid(t.geometry))) as lng_final,
            
            -- Pluscodes (calculados com a localização final)
            coalesce(
                tools.encode_pluscode(
                    coalesce(e.latitude_tratada, st_y(st_centroid(t.geometry))), 
                    coalesce(e.longitude_tratada, st_x(st_centroid(t.geometry))), 
                    11
                ), ''
            ) as plus11,

            -- Identificação principal do equipamento
            t.codigo_unidade as id_equipamento,
            'SMAS' as secretaria_responsavel,
            upper(t.tipo_equipamento) as tipo_equipamento,
            upper(t.nome_oficial) as nome_oficial,
            upper(t.nome_oficial) as nome_popular,

            -- Mais Pluscodes
            coalesce(
                tools.encode_pluscode(
                    coalesce(e.latitude_tratada, st_y(st_centroid(t.geometry))), 
                    coalesce(e.longitude_tratada, st_x(st_centroid(t.geometry))), 
                    10
                ), ''
            ) as plus10,
            coalesce(
                tools.encode_pluscode(
                    coalesce(e.latitude_tratada, st_y(st_centroid(t.geometry))), 
                    coalesce(e.longitude_tratada, st_x(st_centroid(t.geometry))), 
                    8
                ), ''
            ) as plus8,
            coalesce(
                tools.encode_pluscode(
                    coalesce(e.latitude_tratada, st_y(st_centroid(t.geometry))), 
                    coalesce(e.longitude_tratada, st_x(st_centroid(t.geometry))), 
                    6
                ), ''
            ) as plus6,

            -- Geometria (mantém o polígono original)
            t.geometry,

            -- Endereço estruturado (agora com dados reais!)
            struct(
                upper(
                    -- Extrai o logradouro removendo o último elemento (número) após split por espaço
                    trim(regexp_replace(e.endereco, r'\s+[^\s]+$', ''))
                ) as logradouro,
                -- Extrai o número do endereço: split por espaço e pega o último elemento
                -- Se for "S/N" mantém, caso contrário tenta extrair apenas números
                case
                    when upper(trim(split(e.endereco, ' ')[safe_offset(array_length(split(e.endereco, ' ')) - 1)])) = 'S/N'
                    then 'S/N'
                    else trim(split(e.endereco, ' ')[safe_offset(array_length(split(e.endereco, ' ')) - 1)])
                end as numero,
                -- Complemento não está disponível na estrutura atual
                cast(null as string) as complemento,
                upper(e.bairro) as bairro,
                e.cep as cep
            ) as endereco,

            -- Informações de bairro estruturadas (via join espacial usando a localização final)
            struct(
                b.id_bairro as id_bairro,
                upper(b.nome) as bairro,
                upper(b.nome_regiao_planejamento) as regiao_planejamento,
                upper(b.nome_regiao_administrativa) as regiao_administrativa,
                upper(b.subprefeitura) as subprefeitura
            ) as bairro,

            -- Informações de contato estruturadas (agora com dados reais!)
            struct(
                case
                    when e.telefone is not null and trim(e.telefone) != ''
                    then [trim(e.telefone)]
                    else cast([] as array<string>)
                end as telefones,
                e.email_equipamento as email,
                cast(null as string) as site,
                struct(
                    cast(null as string) as facebook,
                    cast(null as string) as instagram,
                    cast(null as string) as twitter
                ) as redes_social
            ) as contato,

            -- Flags de status (usando dados reais quando disponíveis)
            coalesce(
                lower(trim(e.equipamento_ativo)) in ('sim', 's', 'true', '1', 'ativo'),
                true
            ) as ativo,
            coalesce(
                lower(trim(e.equipamento_ativo)) in ('sim', 's', 'true', '1', 'ativo'),
                true
            ) as aberto_ao_publico,

            -- Horário de funcionamento (usando dados reais quando disponíveis)
            case
                when e.dias_de_funcionamento is not null 
                    and e.hora_abertura is not null 
                    and e.hora_fechamento is not null
                then [
                    struct(
                        upper(trim(e.dias_de_funcionamento)) as dia,
                        parse_time('%H:%M', trim(e.hora_abertura)) as abre,
                        parse_time('%H:%M', trim(e.hora_fechamento)) as fecha
                    )
                ]
                else cast([] as array<struct<dia string, abre time, fecha time>>)
            end as horario_funcionamento,

            -- Fonte dos dados
            '{{ source("smas_equipamentos", "poligonos_rmi") }}' as fonte,
            cast(null as date) as vigencia_inicio,
            cast(null as date) as vigencia_fim,

            -- Metadata como JSON (incluindo campos originais e extras)
            to_json_string(
                struct(
                    t.codigo_unidade as codigo_unidade_original,
                    t.secretaria_responsavel as secretaria_original,
                    t.tipo_equipamento as tipo_equipamento_original,
                    e.cas as cas,
                    e.bairros_abrangencia as bairros_abrangencia,
                    e.endereco as endereco_completo_original,
                    e.latitude as latitude_original,
                    e.longitude as longitude_original,
                    case 
                        when e.latitude_tratada is null then 'centroide_poligono'
                        else 'coordenadas_fornecidas'
                    end as fonte_localizacao
                )
            ) as metadata,

            -- Timestamp da última atualização
            current_timestamp() as updated_at

        from base_geometria as t
        left join 
            base_enderecos as e
            on cast(t.codigo_unidade as string) = cast(e.codigo_unidade as string)
        left join
            {{ source("dados_mestres", "bairro") }} as b
            on st_contains(
                b.geometry, 
                st_geogpoint(
                    coalesce(e.longitude_tratada, st_x(st_centroid(t.geometry))),
                    coalesce(e.latitude_tratada, st_y(st_centroid(t.geometry)))
                )
            )
    )

-- Seleção final garantindo a ordem exata das colunas
select
    plus11,
    id_equipamento,
    trim(secretaria_responsavel) as secretaria_responsavel,
    trim(tipo_equipamento) as tipo_equipamento,
    nome_oficial,
    nome_popular,
    plus10,
    plus8,
    plus6,
    lat_final as latitude,
    lng_final as longitude,
    geometry,
    endereco,
    bairro,
    contato,
    ativo,
    aberto_ao_publico,
    horario_funcionamento,
    fonte,
    vigencia_inicio,
    vigencia_fim,
    metadata,
    updated_at
from tb 