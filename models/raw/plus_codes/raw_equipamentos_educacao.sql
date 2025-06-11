
{{
    config(
        alias="equipamentos_educacao",
        schema="plus_codes",
        materialized="table",
    )
}}


WITH tb as (
SELECT
    -- Pluscodes (calculados e ordenados como no exemplo)
    COALESCE(
        tools.encode_pluscode(t.latituasdasdde, t.longitude, 11), ''
    ) AS plus11,

    -- Identificação principal do equipamento
    -- Gerando um ID único caso não exista um na tabela de origem
    FARM_FINGERPRINT(CONCAT(t.cre, t.nome, t.logradouro)) AS id_equipamento,
    'SME' AS secretaria_responsavel, -- Secretaria Municipal de Educação
    UPPER(t.categoria) AS tipo_equipamento,
    UPPER(t.nome) AS nome_oficial,
    UPPER(t.nome) AS nome_popular, -- Usando o nome oficial como popular por falta de um campo específico

    -- Mais Pluscodes
    COALESCE(
        tools.encode_pluscode(t.latituasdasdde, t.longitude, 10), ''
    ) AS plus10,
    COALESCE(
        tools.encode_pluscode(t.latituasdasdde, t.longitude, 8), ''
    ) AS plus8,
    COALESCE(
        tools.encode_pluscode(t.latituasdasdde, t.longitude, 6), ''
    ) AS plus6,

    -- Detalhes de localização
    t.latituasdasdde AS latitude,
    t.longitude AS longitude,
    -- Utilizando a geometria existente na tabela de origem, que é mais confiável.
    -- Caso a coluna t.geometry não seja confiável, pode-se usar a linha comentada abaixo.
    t.geometry,
    -- ST_GEOGPOINT(t.longitude, t.latituasdasdde) as geometry,

    -- Endereço estruturado
    STRUCT(
        UPPER(t.logradouro) AS logradouro,
        t.numero AS numero,
        UPPER(t.complemento) AS complemento,
        UPPER(t.bairro) AS bairro,
        t.cep AS cep
    ) AS endereco,

    -- Informações de bairro estruturadas (via join espacial)
    STRUCT(
        b.id_bairro AS id_bairro,
        b.nome AS bairro,
        b.nome_regiao_planejamento AS regiao_planejamento,
        b.nome_regiao_administrativa AS regiao_administrativa,
        b.subprefeitura AS subprefeitura
    ) AS bairro,

    -- Informações de contato estruturadas
    STRUCT(
        CAST([] AS ARRAY<STRING>) AS telefones, -- Não há campo de telefone na origem
        CAST(NULL AS STRING) AS email, -- Não há campo de email na origem
        CAST(NULL AS STRING) AS site, -- Não há campo de site na origem
        STRUCT(
            CAST(NULL AS STRING) AS facebook,
            CAST(NULL AS STRING) AS instagram,
            CAST(NULL AS STRING) AS twitter
        ) AS redes_social
    ) AS contato,

    -- Flags de status (assumindo que todas as escolas na lista estão ativas)
    TRUE AS ativo,
    TRUE AS aberto_ao_publico,

    -- Horário de funcionamento (não disponível na origem)
    CAST([] AS ARRAY<STRUCT<dia STRING, abre TIME, fecha TIME>>) AS horario_funcionamento,

    -- Fonte dos dados (usando a sintaxe de source do dbt)
    '{{ source("brutos_equipamentos", "escolas") }}' AS fonte,
    CAST(NULL AS DATE) AS vigencia_inicio,
    CAST(NULL AS DATE) AS vigencia_fim,

    -- Metadata como JSON (incluindo colunas não utilizadas diretamente)
    TO_JSON_STRING(
        STRUCT(
            t.cre,
            t.designacao,
            t.rua -- O campo 'rua' parece redundante com 'logradouro', mas o incluímos aqui para não perder a informação
        )
    ) AS metadata,

    -- Timestamp da última atualização
    CURRENT_TIMESTAMP() AS update_at

FROM {{ source("brutos_equipamentos", "escolas") }} AS t
LEFT JOIN {{ source("dados_mestres", "bairro") }} AS b
    -- O join é feito pela geometria da escola contida na geometria do bairro
    ON ST_CONTAINS(b.geometry, t.geometry)
)

-- Seleção final garantindo a ordem exata das colunas
SELECT
    plus11,
    id_equipamento,
    secretaria_responsavel,
    tipo_equipamento,
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
    update_at
FROM tb