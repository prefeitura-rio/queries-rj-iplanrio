{{ config(
    alias="interacoes_pessoa_fisica_areas_planejamento",
    schema="intermediario_dados_mestres",
    materialized=("table" if target.name == "dev" else "ephemeral")
) }}

-- Áreas de Planejamento derivadas dos dados de bairro com geometria agregada

with areas_planejamento_geometria as (
    select 
        id_area_planejamento,
        ANY_VALUE(nome_regiao_planejamento) as primeira_regiao,
        ST_UNION_AGG(geometry) as geometry_uniao,
        SUM(ST_AREA(geometry)) as area_total_m2,
        COUNT(*) as total_bairros
    from {{ ref('raw_iplanrio_dados_mestres__bairro') }}
    where geometry is not null
    group by id_area_planejamento
)

select
    concat('ap_', id_area_planejamento) as id_divisao,
    'AREA_PLANEJAMENTO' as tipo_divisao,
    cast(id_area_planejamento as string) as codigo_original,
    concat('AP', id_area_planejamento) as nome,
    concat('AP', id_area_planejamento) as nome_abreviado,
    'IPP' as orgao_responsavel,
    cast(['planejamento_territorial'] as array<string>) as competencias,
    cast(null as date) as data_criacao,
    'Plano Diretor Municipal' as legislacao_base,
    area_total_m2 as area_m2,
    ST_PERIMETER(geometry_uniao) as perimetro_m,
    ST_Y(ST_CENTROID(geometry_uniao)) as centroide_latitude,
    ST_X(ST_CENTROID(geometry_uniao)) as centroide_longitude,
    geometry_uniao as geometry,
    ST_ASTEXT(geometry_uniao) as geometry_wkt,
    cast(null as float64) as densidade_populacional,
    'misto' as uso_solo_predominante,
    cast([] as array<string>) as restricoes_urbanisticas,
    cast(['planejamento_urbano'] as array<string>) as instrumentos_urbanisticos,
    to_json(struct(
        cast(null as string) as caracteristicas_socioeconomicas,
        cast([] as array<string>) as diretrizes_planejamento,
        total_bairros as numero_bairros
    )) as atributos_especificos,
    to_json(struct(
        'bairro_aggregated' as tabela_origem,
        total_bairros as bairros_agregados
    )) as metadados_fonte,
    true as ativo,
    current_timestamp() as data_atualizacao,
    'rj-escritorio-dev.dados_mestres.bairro' as fonte_dados,
    '1.0' as versao_schema
from areas_planejamento_geometria