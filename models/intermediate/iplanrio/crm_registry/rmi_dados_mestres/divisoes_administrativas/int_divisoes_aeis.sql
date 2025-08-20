{{ config(
    alias="interacoes_pessoa_fisica_aeis",
    schema="intermediario_dados_mestres",
    materialized=("table" if target.name == "dev" else "ephemeral")
) }}

-- Áreas de Especial Interesse Social como divisões especializadas

with numbered_aeis as (
    select *,
        row_number() over (
            partition by coalesce(nullif(trim(id_aeis), ''), 'sem_id') 
            order by nome_aeis, data_cadastro
        ) as rn
    from {{ ref('raw_iplanrio_dados_mestres__aeis') }}
)

select
    case 
        when id_aeis is not null and trim(id_aeis) != '' 
        then concat('aeis_', trim(id_aeis), '_', rn)
        else concat('aeis_sem_id_', row_number() over (order by nome_aeis, data_cadastro))
    end as id_divisao,
    'AEIS' as tipo_divisao,
    cast(id_aeis as string) as codigo_original,
    nome_aeis as nome,
    nome_sigla as nome_abreviado,
    'SMU' as orgao_responsavel,
    cast(['habitacao_social', 'regularizacao_fundiaria'] as array<string>) as competencias,
    data_cadastro as data_criacao,
    legislacao as legislacao_base,
    safe_cast(area as float64) as area_m2,
    safe_cast(comprimento as float64) as perimetro_m,
    st_y(st_centroid(geometry)) as centroide_latitude,
    st_x(st_centroid(geometry)) as centroide_longitude,
    geometry,
    geometria_wkt as geometry_wkt,
    cast(null as float64) as densidade_populacional,
    'habitacao_social' as uso_solo_predominante,
    case 
        when tipologia is not null then cast([tipologia] as array<string>) 
        else cast([] as array<string>) 
    end as restricoes_urbanisticas,
    cast(['AEIS'] as array<string>) as instrumentos_urbanisticos,
    to_json(struct(
        tipologia as tipologia_aeis,
        item,
        nome_setor,
        nome_ar,
        planta_cadastral,
        projeto_loteamento,
        referencia,
        limite_pavimentos_permitido,
        taxa_ocupacao,
        indice_aproveitamento_terreno,
        coeficiente_adensamento,
        afastamento,
        cast(null as string) as situacao_regularizacao,
        cast(null as string) as projeto_urbanizacao,
        legislacao as legislacao_especifica
    )) as atributos_especificos,
    to_json(struct(
        'aeis' as tabela_origem,
        nome_regiao_administrativa
    )) as metadados_fonte,
    true as ativo,
    current_timestamp() as data_atualizacao,
    'rj-escritorio-dev.dados_mestres.aeis' as fonte_dados,
    '1.0' as versao_schema
from numbered_aeis