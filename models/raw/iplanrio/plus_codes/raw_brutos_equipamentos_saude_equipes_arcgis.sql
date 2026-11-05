
{{
    config(
        alias="saude_equipe_familia",
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
  area_planejamento,
  'EQUIPE DA FAMILIA' as categoria,
  nome_area,
  id_equipe,
  id_ine,
  tipo_equipe,
  medicos,
  enfermeiros,
  telefone_equipe as telefone,
  cnes,
  geometry
FROM base

