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


    equipamentos as (
        select *
        from {{ ref("raw_equipamentos") }}
    ),

    equipamentos_proximidade as (
        select *
        from equipamentos
        where fonte not in (
            '{{ ref("raw_equipamentos_saude_unidades_arcgis") }}', 
            '{{ ref("raw_equipamentos_saude_equipes_arcgis") }}',
            '{{ source("smas_equipamentos", "poligonos_rmi") }}'
        )
    ),
    
    -- 2) Pares grid × equipamento dentro do raio
    pairs_proximidade as (
        select
            g.plus8,
            g.centro_geometry,
            e.secretaria_responsavel,
            e.categoria,

            -- Struct contendo TODA a linha do equipamento + distância
            (
                select as struct
                        e.plus8,
                        e.plus11,
                        e.id_equipamento,
                        e.geometry,
                        e.secretaria_responsavel,
                        e.categoria,
                        e.use,
                        e.tipo_equipamento,
                        e.nome_oficial,
                        e.nome_popular,
                        e.plus10,
                        e.plus6,
                        e.latitude,
                        e.longitude,
                        e.endereco,
                        e.bairro,
                        e.contato,
                        e.ativo,
                        e.aberto_ao_publico,
                        e.horario_funcionamento,
                        e.fonte,
                        e.vigencia_inicio,
                        e.vigencia_fim,
                        e.esfera,
                        e.metadata,
                        e.updated_at,
                        st_distance(e.geometry, g.centro_geometry) as distancia_metros
            ) as equip_full
        from grid as g
        join
            equipamentos_proximidade as e
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
        from pairs_proximidade
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
