{{
    config(
        alias="qmd_servicos",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "qmd_servicos") }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível
            where
                safe_cast(data_particao as date) = (
                    select max(safe_cast(data_particao as date))
                    from {{ source("brutos_forca_municipal_staging", "qmd_servicos") }}
                )
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id("id_hash") }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(data_particao as date) as data_particao,

            -- identificadores (FKs para outras entidades)
            {{ padronize_id("IdPlano") }} as id_plano,
            {{ padronize_id("IdQmd") }} as id_qmd,

            -- dados
            {{ padronize_id("Id") }} as id_servico,
            upper(trim(safe_cast(nome as string))) as id_unidade,
            array(
                select as struct
                    case
                        json_value(dia, '$')
                        when 'seg' then 'Segunda'
                        when 'ter' then 'Terça'
                        when 'qua' then 'Quarta'
                        when 'qui' then 'Quinta'
                        when 'sex' then 'Sexta'
                        when 'sab' then 'Sábado'
                        when 'dom' then 'Domingo'
                    end as week_day,
                    case
                        json_value(dia, '$')
                        when 'seg' then 2
                        when 'ter' then 3
                        when 'qua' then 4
                        when 'qui' then 5
                        when 'sex' then 6
                        when 'sab' then 7
                        when 'dom' then 1
                    end as week_day_number
                from unnest(json_query_array(safe_cast(dias as string))) as dia
            ) as dias,
            {{ tipo_unidade("Nome") }} as tipo_unidade,
            {{ base_operacional("Nome") }} as base_operacional

        from source
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(updated_at) as updated_at,
            max(data_particao) as data_particao,

            -- identificadores
            any_value(id_plano) as id_plano,
            any_value(id_qmd) as id_qmd,

            -- dados
            any_value(id_servico) as id_servico,
            any_value(id_unidade) as id_unidade,
            any_value(dias) as dias,
            any_value(tipo_unidade) as tipo_unidade,
            any_value(base_operacional) as base_operacional
        from renamed
        group by id_hash
    )

select *
from deduplicado