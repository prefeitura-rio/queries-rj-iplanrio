{{ config(
    alias="interacoes_pessoa_fisica_subprefeituras",
    schema="intermediario_dados_mestres",
    materialized=("table" if target.name == "dev" else "ephemeral")
) }}

-- Subprefeituras como divisões administrativas independentes

select
    concat('subpref_', regexp_replace(lower(subprefeitura), r'[^a-z0-9]', '_')) as id_divisao,
    'SUBPREFEITURA' as tipo_divisao,
    cast(subprefeitura as string) as codigo_original,
    subprefeitura as nome,
    cast(null as string) as nome_abreviado,
    'SUBGAB' as orgao_responsavel,
    cast(['administracao_regional', 'servicos_publicos_locais'] as array<string>) as competencias,
    cast(null as date) as data_criacao,
    cast(null as string) as legislacao_base,
    area as area_m2,
    perimetro as perimetro_m,
    st_y(st_centroid(geometria)) as centroide_latitude,
    st_x(st_centroid(geometria)) as centroide_longitude,
    geometria as geometry,
    geometry_wkt,
    cast(null as float64) as densidade_populacional,
    cast(null as string) as uso_solo_predominante,
    cast([] as array<string>) as restricoes_urbanisticas,
    cast([] as array<string>) as instrumentos_urbanisticos,
    to_json(struct(
        cast(null as string) as endereco_sede,
        cast(null as string) as telefone_contato,
        cast(null as string) as email_contato,
        cast(null as string) as horario_funcionamento
    )) as atributos_especificos,
    to_json(struct(
        'subprefeitura' as tabela_origem
    )) as metadados_fonte,
    true as ativo,
    current_timestamp() as data_atualizacao,
    'rj-escritorio-dev.dados_mestres.subprefeitura' as fonte_dados,
    '1.0' as versao_schema
from {{ ref('raw_iplanrio_dados_mestres__subprefeitura') }}