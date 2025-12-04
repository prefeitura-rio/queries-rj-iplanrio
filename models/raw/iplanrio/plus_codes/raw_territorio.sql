{{
    config(
        alias="territorio",
        schema="plus_codes",
        materialized="table",
        cluster_by = ["secretaria_responsavel"],
        partition_by={
            "field": "categoria",
            "data_type": "string"
        }
    )
}}


with
    equipamentos as (
        select *
        from {{ ref("raw_equipamentos") }}
    ),
    equipamentos_territorio as (
        select *
        from equipamentos e
        where fonte in (
            '{{ ref("raw_equipamentos_saude_unidades_arcgis") }}', 
            '{{ ref("raw_equipamentos_saude_equipes_arcgis") }}',
            '{{ source("smas_equipamentos", "poligonos_rmi") }}'
        )
    ),
pair_territorio as (
        select 
            e.secretaria_responsavel,
            e.categoria,
            e.geometry,
        (
            select as struct
                    e.plus8,
                    e.plus11,
                    e.id_equipamento,
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
                    e.esfera,
                    e.horario_funcionamento,
                    e.fonte,
                    e.vigencia_inicio,
                    e.vigencia_fim,
                    e.metadata,
                    e.updated_at,
        ) as equipamentos
        from equipamentos_territorio e
        WHERE categoria is not null
    )

select
    categoria,
    secretaria_responsavel,
    geometry,
    equipamentos,
from pair_territorio





















{# WITH  grid as (
        select plus8, geometry as centro_geometry
        from `rj-iplanrio`.`plus_codes`.`grid`
    ),

    equipamentos as (
        select *
        from `rj-iplanrio`.`plus_codes`.`equipamentos`
    ),


        equipamentos_territorio as (
        select *
        from `rj-iplanrio`.`plus_codes`.`equipamentos` e
        where fonte in ('`rj-iplanrio`.`brutos_equipamentos`.`saude_equipe_familia`')
        ),

        tb as (
          select 
            g.plus8,
            g.centro_geometry,
            e.secretaria_responsavel,
            e.categoria,
            e.id_equipamento,
        (
            select as struct
                e.*
        ) as equip_full
        from grid g
        left join equipamentos_territorio e
            on ST_CONTAINS(e.geometry, g.centro_geometry)
        )
        
SELECT 
  e.*
FROM equipamentos_territorio e
LEFT JOIN tb t
on  #}
