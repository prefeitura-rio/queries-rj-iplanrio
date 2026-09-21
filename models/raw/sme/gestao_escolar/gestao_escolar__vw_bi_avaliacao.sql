{{
    config(
        alias="vw_bi_avaliacao",
        schema="gestao_escolar",
    )
}}

with
    source as (select * from {{ source("brutos_gestao_escolar_staging_prefect", "VW_BI_Avaliacao") }}),
    renamed as (
        select
            {{ dbt_utils.generate_surrogate_key(["alu_id", "ano", "coc", "mtu_id"]) }}
            as id,
            
             {{ dbt_utils.generate_surrogate_key(["alu_id", "ano", "coc"]) }}
            as id_aluno_ano_coc,

            * except (frequencia, freq_acumulada),

            frequencia as frequencia_str,
            safe_cast(frequencia as float64) as frequencia,

            freq_acumulada as freq_acumulada_str,
            safe_cast(freq_acumulada as float64) as freq_acumulada

        from source
    )
    
select *
from renamed
where frequencia is not null and id not in ('422e6b0792d77d7c531d6accdd25e3aa')
