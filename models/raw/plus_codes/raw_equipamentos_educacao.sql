{{
    config(
        alias="equipamentos_educacao",
        schema="plus_codes",
        materialized="table",
    )
}}


with
    tb as (
        select
            -- Pluscodes (calculados e ordenados como no exemplo)
            coalesce(
                tools.encode_pluscode(t.latituasdasdde, t.longitude, 11), ''
            ) as plus11,

            -- Identificação principal do equipamento
            -- Gerando um ID único caso não exista um na tabela de origem
            cast(null as string) as id_equipamento,
            'SME' as secretaria_responsavel,  -- Secretaria Municipal de Educação
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
            -- Utilizando a geometria existente na tabela de origem, que é mais
            -- confiável.
            -- Caso a coluna t.geometry não seja confiável, pode-se usar a linha
            -- comentada abaixo.
            t.geometry,
            -- ST_GEOGPOINT(t.longitude, t.latituasdasdde) as geometry,
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

            -- Informações de contato estruturadas
            struct(
                cast([] as array<string>) as telefones,  -- Não há campo de telefone na origem
                cast(null as string) as email,  -- Não há campo de email na origem
                cast(null as string) as site,  -- Não há campo de site na origem
                struct(
                    cast(null as string) as facebook,
                    cast(null as string) as instagram,
                    cast(null as string) as twitter
                ) as redes_social
            ) as contato,

            -- Flags de status (assumindo que todas as escolas na lista estão ativas)
            true as ativo,
            true as aberto_ao_publico,

            -- Horário de funcionamento (não disponível na origem)
            cast(
                [] as array<struct<dia string, abre time, fecha time>>
            ) as horario_funcionamento,

            -- Fonte dos dados (usando a sintaxe de source do dbt)
            '{{ source("brutos_equipamentos", "escolas") }}' as fonte,
            cast(null as date) as vigencia_inicio,
            cast(null as date) as vigencia_fim,

            -- Metadata como JSON (incluindo colunas não utilizadas diretamente)
            to_json_string(
                struct(
                    t.cre, t.designacao, t.rua  -- O campo 'rua' parece redundante com 'logradouro', mas o incluímos aqui para não perder a informação
                )
            ) as metadata,

            -- Timestamp da última atualização
            current_timestamp() as updated_at

        from {{ ref("raw_equipamentos_escolas") }} as t
        left join
            {{ source("dados_mestres", "bairro") }} as b
            -- O join é feito pela geometria da escola contida na geometria do bairro
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
