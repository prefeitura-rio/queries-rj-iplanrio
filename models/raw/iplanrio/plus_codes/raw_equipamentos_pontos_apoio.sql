{{
    config(
        alias="equipamentos_pontos_apoio",
        schema="plus_codes",
        materialized="table",
    )
}}

with
    base_columbia as (
        select * from {{ source("plus_codes_dev", "equipamentos_columbia") }}
        where nome is not null
    ),
    
    base_columbia_treated as (
        select
            *,
            -- Tratar latitude: dividir por 10^8 para obter coordenadas com 8 casas decimais
            cast(lat as float64) / 100000000 as lat_tratada,
            -- Tratar longitude: dividir por 10^8 para obter coordenadas com 8 casas decimais  
            cast(lon as float64) / 100000000 as lon_tratada
        from base_columbia
        where lat is not null 
          and lon is not null
    ),
    
    tb as (
        select
            -- Coordenadas finais (tratadas)
            c.lat_tratada as lat_final,
            c.lon_tratada as lng_final,
            
            -- Pluscodes (calculados com as coordenadas tratadas)
            coalesce(
                tools.encode_pluscode(
                    c.lat_tratada, 
                    c.lon_tratada, 
                    11
                ), ''
            ) as plus11,

            -- Identificação principal do equipamento
            -- Hash do nome + logradouro para ID único
            to_hex(md5(concat(upper(trim(c.nome)), '|', upper(trim(c.logradouro))))) as id_equipamento,
            'COR' as secretaria_responsavel,
            'PONTOS_DE_APOIO' as tipo_equipamento,
            upper(trim(c.nome)) as nome_oficial,
            upper(trim(c.nome)) as nome_popular,

            -- Mais Pluscodes
            coalesce(
                tools.encode_pluscode(
                    c.lat_tratada, 
                    c.lon_tratada, 
                    10
                ), ''
            ) as plus10,
            coalesce(
                tools.encode_pluscode(
                    c.lat_tratada, 
                    c.lon_tratada, 
                    8
                ), ''
            ) as plus8,
            coalesce(
                tools.encode_pluscode(
                    c.lat_tratada, 
                    c.lon_tratada, 
                    6
                ), ''
            ) as plus6,

            -- Geometria (ponto criado a partir das coordenadas tratadas)
            st_geogpoint(c.lon_tratada, c.lat_tratada) as geometry,

            -- Endereço estruturado (usando dados disponíveis)
            struct(
                upper(trim(c.logradouro)) as logradouro,
                case
                    when trim(c.numero) is not null and trim(c.numero) != ''
                    then trim(c.numero)
                    else 'S/N'
                end as numero,
                cast(null as string) as complemento,
                upper(trim(c.bairro)) as bairro,
                trim(c.cep) as cep
            ) as endereco,

            -- Informações de bairro estruturadas (via join espacial)
            struct(
                b.id_bairro as id_bairro,
                upper(b.nome) as bairro,
                upper(b.nome_regiao_planejamento) as regiao_planejamento,
                upper(b.nome_regiao_administrativa) as regiao_administrativa,
                upper(b.subprefeitura) as subprefeitura
            ) as bairro,

            -- Informações de contato estruturadas (não disponíveis)
            struct(
                cast([] as array<string>) as telefones,
                cast(null as string) as email,
                cast(null as string) as site,
                struct(
                    cast(null as string) as facebook,
                    cast(null as string) as instagram,
                    cast(null as string) as twitter
                ) as redes_social
            ) as contato,

            -- Flags de status (padrão: ativos e abertos ao público)
            true as ativo,
            true as aberto_ao_publico,

            -- Horário de funcionamento (não disponível)
            cast([] as array<struct<dia string, abre time, fecha time>>) as horario_funcionamento,

            -- Fonte dos dados
            '{{ source("plus_codes_dev", "equipamentos_columbia") }}' as fonte,
            cast(null as date) as vigencia_inicio,
            cast(null as date) as vigencia_fim,

            -- Metadata como JSON (incluindo campos originais)
            to_json_string(
                struct(
                    c.nome as nome_original,
                    c.logradouro as logradouro_original,
                    c.numero as numero_original,
                    c.bairro as bairro_original,
                    c.cep as cep_original,
                    c.cidade as cidade,
                    c.uf as uf,
                    c.lat as latitude_original,
                    c.lon as longitude_original,
                    c.lat_tratada as latitude_tratada,
                    c.lon_tratada as longitude_tratada,
                    'coordenadas_fornecidas' as fonte_localizacao
                )
            ) as metadata,

            -- Timestamp da última atualização
            current_timestamp() as updated_at

        from base_columbia_treated as c
        left join
            {{ source("dados_mestres", "bairro") }} as b
            on st_contains(
                b.geometry, 
                st_geogpoint(c.lon_tratada, c.lat_tratada)
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