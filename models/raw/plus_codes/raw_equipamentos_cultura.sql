{{
    config(
        alias="equipamentos_cultura",
        schema="plus_codes",
        materialized="table",
    )
}}

{# Força dependência da UDF encode_pluscode #}
{# {{ ref('encode_pluscode') }} #}


with
    tb as (
        select
            -- Pluscodes (calculados e ordenados como no exemplo)
            coalesce(
                tools.encode_pluscode(t.latituasdasdde, t.longitude, 11), ''
            ) as plus11,

            -- Identificação principal do equipamento
            cast(null as string) as id_equipamento,  -- Mantendo o padrão do modelo anterior
            'SMC' as secretaria_responsavel,  -- Secretaria Municipal de Cultura
            upper(t.categoria) as tipo_equipamento,
            upper(t.nome) as nome_oficial,
            upper(t.nome) as nome_popular,  -- Usando o nome oficial como popular por falta de um campo específico

            -- Mais Pluscodes
            coalesce(
                tools.encode_pluscode(t.latituasdasdde, t.longitude, 10), ''
            ) as plus10,
            coalesce(
                tools.encode_pluscode(t.latituasdasdde, t.longitude, 8), ''
            ) as plus8,
            coalesce(
                tools.encode_pluscode(t.latituasdasdde, t.longitude, 6), ''
            ) as plus6,

            -- Detalhes de localização
            t.latituasdasdde as latitude,
            t.longitude as longitude,
            t.geometry,

            -- Endereço estruturado
            struct(
                upper(t.logradouro) as logradouro,
                t.numero as numero,
                upper(t.complemento) as complemento,
                upper(t.bairro) as bairro,
                t.cep as cep
            ) as endereco,

            -- Informações de bairro estruturadas (via join espacial)
            struct(
                b.id_bairro as id_bairro,
                upper(b.nome) as bairro,
                upper(b.nome_regiao_planejamento) as regiao_planejamento,
                upper(b.nome_regiao_administrativa) as regiao_administrativa,
                upper(b.subprefeitura) as subprefeitura
            ) as bairro,

            -- Informações de contato estruturadas (nulas, pois não existem na origem)
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

            -- Flags de status (assumindo que todos os equipamentos na lista estão
            -- ativos)
            true as ativo,
            true as aberto_ao_publico,

            -- Horário de funcionamento (não disponível na origem)
            cast(
                [] as array<struct<dia string, abre time, fecha time>>
            ) as horario_funcionamento,

            -- Fonte dos dados (ajuste o nome da tabela 'cultura' se necessário)
            '{{ ref("raw_equipamentos_culturais") }}' as fonte,
            cast(null as date) as vigencia_inicio,
            cast(null as date) as vigencia_fim,

            -- Metadata como JSON (incluindo colunas não utilizadas diretamente)
            to_json_string(struct(t.id_equipamento, t.rua_original)) as metadata,

            -- Timestamp da última atualização
            current_timestamp() as updated_at

        from {{ ref("raw_equipamentos_culturais") }} as t
        left join
            {{ source("dados_mestres", "bairro") }} as b
            -- O join é feito pela geometria do equipamento contida na geometria do
            -- bairro
            on st_contains(b.geometry, t.geometry)
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
    latitude,
    longitude,
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
