{{ config(
    alias="interacoes_pessoa_fisica_bairros",
    schema="intermediario_dados_mestres",
    materialized=("table" if target.name == "dev" else "ephemeral")
) }}

-- Bairros do Rio de Janeiro como divisões administrativas independentes

select
    concat('bairro_', id_bairro) as id_divisao,
    'BAIRRO' as tipo_divisao,
    cast(id_bairro as string) as codigo_original,
    nome as nome,
    cast(null as string) as nome_abreviado,
    'IPP' as orgao_responsavel,
    cast(['gestao_territorial', 'servicos_locais'] as array<string>) as competencias,
    cast(null as date) as data_criacao,
    cast(null as string) as legislacao_base,
    area as area_m2,
    perimetro as perimetro_m,
    st_y(st_centroid(geometry)) as centroide_latitude,
    st_x(st_centroid(geometry)) as centroide_longitude,
    geometry,
    geometry_wkt,
    cast(null as float64) as densidade_populacional,
    cast(null as string) as uso_solo_predominante,
    cast([] as array<string>) as restricoes_urbanisticas,
    cast([] as array<string>) as instrumentos_urbanisticos,
    to_json(struct(
        cast(id_bairro as string) as codigo_bairro,
        cast(null as string) as caracteristicas_urbanas
    )) as atributos_especificos,
    to_json(struct(
        'bairro' as tabela_origem,
        cast(id_area_planejamento as string) as id_area_planejamento,
        nome_regiao_planejamento,
        cast(id_regiao_administrativa as string) as id_regiao_administrativa,
        nome_regiao_administrativa,
        subprefeitura
    )) as metadados_fonte,
    true as ativo,
    current_timestamp() as data_atualizacao,
    'rj-escritorio-dev.dados_mestres.bairro' as fonte_dados,
    '1.0' as versao_schema
from {{ ref('raw_iplanrio_dados_mestres__bairro') }}