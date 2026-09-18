{{
    config(
        alias="ocorrencias",
        schema="brutos_ispgeo",
        materialized="table",
    )
}}

with
    source as (
        select *
        from {{ source('brutos_ispgeo_staging', 'ocorrencias') }}
        {% if is_incremental() %}
            where safe_cast(data_particao as date) >= (
                select max(data_particao) from {{ this }}
            )
        {% endif %}
    ),

    renamed as (
        select
            -- identificadores
            safe_cast(objectid as int64) as objectid,
            safe_cast(target_fid as int64) as target_fid,
            safe_cast(chave as string) as chave,
            safe_cast(rgocronu as string) as RO,
            safe_cast(uuid as string) as uuid,

            -- delito
            safe_cast(etit as int64) as titulo_delito,
            safe_cast(eseq as int64) as sequencial_envolvido,
            safe_cast(delito_do as int64) as titulo_do,
            safe_cast(sim as int64) as indicador_estrategico,
            safe_cast(total_rbft as int64) as total_rbft,
            safe_cast(fase as int64) as fase_divulgacao,

            -- tempo do registro
            safe_cast(ano as int64) as ano_registro,
            safe_cast(mes as int64) as mes_registro,
            safe_cast(datc as date) as data_registro,

            -- tempo do fato
            safe_cast(datf as date) as data_fato,
            safe_cast(ano_fato as int64) as ano_fato,
            safe_cast(id_mes_fato as int64) as id_mes_fato,
            safe_cast(mes_fato as string) as mes_fato,
            safe_cast(horf as string) as hora_fato,
            safe_cast(hora_faixa as string) as hora_fato_faixa,
            safe_cast(fhora as int64) as hora_fato_sem_minutos,
            safe_cast(fdiasem as int64) as dia_semana_fato,
            safe_cast(id_dia_semana_fato as int64) as id_dia_semana_fato,
            safe_cast(dia_semana_fato as string) as dia_semana_fato_abrev,
            safe_cast(ffaixa as int64) as faixa_horaria,
            safe_cast(periodo_dia as string) as periodo_dia,

            -- área de segurança
            safe_cast(cisp as int64) as cisp,
            safe_cast(aisp as int64) as aisp,
            safe_cast(risp as int64) as risp,

            -- localização
            safe_cast(ftlc as int64) as tipo_local_fato,
            safe_cast(fmun_cod as int64) as municipio_fato_ibge,
            safe_cast(municipio as string) as municipio_fato,
            safe_cast(fcom as string) as complemento_endereco,
            safe_cast(localidade as string) as localidade,
            safe_cast(ftlo_recode as string) as tipo_logradouro,
            safe_cast(flog_recode as string) as nome_logradouro,
            safe_cast(locf_recode as string) as logradouro_completo,
            safe_cast(fnum_recode as int64) as num_porta,
            safe_cast(fref_recode as string) as ponto_referencia,
            safe_cast(fbai_recode as string) as bairro,
            safe_cast(intersecao as string) as intersecao_esquina,
            safe_cast(km as int64) as km,
            safe_cast(endereco as string) as logradouro_numerica,
            safe_cast(endereco_sem_tipo as string) as endereco_sem_tipo,
            safe_cast(esquina as string) as esquina,

            -- geocodificação
            safe_cast(lat as float64) as latitude,
            safe_cast(long as float64) as longitude,
            safe_cast(point_x as float64) as point_x,
            safe_cast(point_y as float64) as point_y,
            safe_cast(celula as int64) as celula_ibge,
            safe_cast(class_geocode as int64) as class_geocode,
            safe_cast(wkt as string) as wkt,
            safe_cast(geography as string) as geography,

            -- foco especial / território
            safe_cast(id_territorio as int64) as id_territorio,
            safe_cast(distancia_focoespecial as float64) as distancia_area_foco_especial,
            safe_cast(nome_focoespecial as string) as nome_area_foco_especial,
            safe_cast(dominio_focoespecial as string) as dominio_focoespecial

        from source
    )

select *
from renamed
