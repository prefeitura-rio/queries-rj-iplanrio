{{
    config(
        alias="codes",
        schema="plus_codes",
        materialized="table",
        cluster_by = ["categoria"],
        partition_by={
            "field": "plus8",
            "data_type": "string"
        }
    )
}}


with
    grid as (
        select plus8, geometry as centro_geometry
        from {{ source('plus_codes', 'grid') }}
    ),

    -- 2) Pares grid × equipamento dentro do raio
    pairs as (
        select
            g.plus8,
            g.centro_geometry,
            e.secretaria_responsavel,
            e.categoria,

            -- Struct contendo TODA a linha do equipamento + distância
            (
                select as struct
                    e.*,  -- todos os campos de equipamentos_geo
                    st_distance(e.geometry, g.centro_geometry) as distancia_metros
            ) as equip_full
        from grid as g
        join
            {{ ref("raw_equipamentos") }} as e
            on st_dwithin(e.geometry, g.centro_geometry, 100000000)
    ),

    -- 3) Rankeia por distância
    ranqueado as (
        select
            plus8,
            categoria,
            secretaria_responsavel,
            equip_full.distancia_metros as distancia_metros,
            centro_geometry,
            equip_full,
            row_number() over (
                partition by plus8, secretaria_responsavel, categoria
                order by equip_full.distancia_metros
            ) as rn
        from pairs
    )

-- 4) Agrega os 3 mais próximos
select
    plus8,
    trim(secretaria_responsavel) as secretaria_responsavel,
    trim(categoria) as categoria,
    any_value(centro_geometry) as geometry,
    array_agg(equip_full order by distancia_metros limit 1) as equipamentos,
    current_timestamp() as ingestion_timestamp
from ranqueado
where rn <= 1
group by plus8, secretaria_responsavel, categoria
