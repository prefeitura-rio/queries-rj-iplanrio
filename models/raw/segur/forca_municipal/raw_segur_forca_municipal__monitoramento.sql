{{
    config(
        alias="monitoramento",
        schema="brutos_forca_municipal",
        materialized="table",
    )
}}

with
    particoes as (
        select
            table_name as tabela,
            partition_id,
            total_rows,
            total_logical_bytes,
            last_modified_time,
            max(partition_id) over (partition by table_name) as ultima_partition_id,
        from `rj-segur.brutos_forca_municipal.INFORMATION_SCHEMA.PARTITIONS`
        where partition_id not in ('__NULL__', '__UNPARTITIONED__')
    ),

    por_tabela as (
        select
            tabela,
            parse_date('%Y%m%d', max(partition_id)) as ultima_data_particao,
            sum(if(partition_id = ultima_partition_id, total_rows, 0))
                as linhas_ultima_particao,
            sum(total_rows) as total_linhas,
            max(last_modified_time) as ultima_modificacao,
            round(sum(total_logical_bytes) / pow(1024, 3), 6) as total_gigabytes,
        from particoes
        group by tabela
    )

select
    tabela,
    ultima_data_particao,
    date_diff(
        current_date('America/Sao_Paulo'), ultima_data_particao, day
    ) as dias_atraso,
    linhas_ultima_particao,
    total_linhas,
    datetime(ultima_modificacao, 'America/Sao_Paulo') as ultima_modificacao,
    total_gigabytes,
from por_tabela
order by tabela
