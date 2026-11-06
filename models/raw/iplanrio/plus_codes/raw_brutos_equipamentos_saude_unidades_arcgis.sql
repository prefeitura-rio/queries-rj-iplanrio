
{{
    config(
        alias="saude_unidades",
        schema="brutos_equipamentos",
        materialized="table",
    )
}}

with base as (
    SELECT
        CAP as cap,
        CNES as cnes,
        COD_Equipe as id_equipe,
        COD_INE as id_ine,
        NOME_FANTASIA as nome_unidade,
        AP as area_planejamento,
        TIPO_UNIDADE_APS as categoria,
        LOGRADOURO as logradouro,
        NUMERO as numero,
        COMPLEMENTO as complemento,
        COD_CEP as cep,
        TELEFONE as telefone,
        CELULAR as celular,
        E_MAIL as email,
        E_MAIL2 as email2,
        SITE as site,
        HORA1 as hora_abre,
        HORA2 as hora_fecha,
        HORASAB1 as hora_abre_sabado,
        HORASAB2 as hora_fecha_sabado,
        COD_AREA as id_area,
        NOME_AREA as nome_area,
        DT_ATIVA as data_ativa,
        MEDICOS as medicos,
        ENFERMEIROS as enfermeiros,
        BAIRRO as bairro,
        COD_INE_1 as cod_ine_1,
        TIPO_EQP as tipo_equipe,
        F24horas as funciona_24_horas,
        END_ as endereco,
        TELEFONE_EQP as telefone_equipe,
        FACEBOOK as facebook,
        INSTAGRAM as instagram,
        TWITTER as twitter,
        ST_GEOGFROMTEXT(geometry, make_valid => TRUE) as geometry
    FROM {{ source("brutos_equipamentos_staging", "unidades_saude_poligonos_arcgis") }}
)

SELECT 
  b.cnes,
  b.area_planejamento,
  b.nome_unidade,
  b.categoria,
  b.logradouro,
  b.numero,
  b.complemento,
  b.cep,
  b.bairro,
  b.telefone,
  b.email,
  b.email2,
  b.site,
  b.hora_abre,
  b.hora_fecha,
  b.hora_abre_sabado,
  b.hora_fecha_sabado,
  b.funciona_24_horas,
  b.facebook,
  b.instagram,
  b.twitter,
  ST_GEOGFROMTEXT(u.geometry, make_valid => TRUE) as geometry_unidade,
  CAST(u.latitude as FLOAT64) as latitude_unidade,
  CAST(u.longitude as FLOAT64) as longitude_unidade,
  b.geometry
FROM base b
left join {{ source("brutos_equipamentos_staging", "unidades_saude_arcgis") }} u
    on b.cnes = u.cnes