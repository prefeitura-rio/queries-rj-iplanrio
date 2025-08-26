{{ config(
    alias="interacoes_pessoa_fisica_cres",
    schema="intermediario_dados_mestres",
    materialized=("table" if target.name == "dev" else "ephemeral")
) }}

-- Coordenadorias Regionais de Educação como divisões especializadas

select
    concat('cre_', id) as id_divisao,
    'CRE' as tipo_divisao,
    cast(id as string) as codigo_original,
    nome as nome,
    concat('CRE ', id) as nome_abreviado,
    'SME' as orgao_responsavel,
    cast(['coordenacao_educacional', 'gestao_escolar'] as array<string>) as competencias,
    cast(null as date) as data_criacao,
    cast(null as string) as legislacao_base,
    st_area(geometry) as area_m2,
    st_perimeter(geometry) as perimetro_m,
    st_y(st_centroid(geometry)) as centroide_latitude,
    st_x(st_centroid(geometry)) as centroide_longitude,
    geometry,
    geometry_wkt,
    cast(null as float64) as densidade_populacional,
    'educacional' as uso_solo_predominante,
    cast([] as array<string>) as restricoes_urbanisticas,
    cast([] as array<string>) as instrumentos_urbanisticos,
    to_json(struct(
        cast(null as int64) as numero_escolas,
        cast(null as int64) as numero_alunos,
        cast(null as string) as coordenador_regional
    )) as atributos_especificos,
    to_json(struct(
        'cres' as tabela_origem
    )) as metadados_fonte,
    true as ativo,
    current_timestamp() as data_atualizacao,
    'rj-escritorio-dev.dados_mestres.cres' as fonte_dados,
    '1.0' as versao_schema
from {{ ref('raw_iplanrio_dados_mestres__cres') }}