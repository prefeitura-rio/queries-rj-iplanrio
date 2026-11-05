with base as (
    SELECT
        CNES as cnes,
        NOME as nome,
        CAP as cap,
        EQUIPES as equipes,
        ENDERECO as endereco,
        BAIRRO as bairro,
        TELEFONE as telefone,
        EMAIL as email,
        HORARIO_SEMANA as horario_semana,
        HORARIO_SABADO as horario_sabado,
        TIPO_ABC as tipo_abc,
        TIPO_UNIDADE as tipo_unidade,
        DATA_INAUGURACAO as data_inauguracao,
        Flg_Ativo as flg_ativo,
        CNES_ as cnes_,
        GlobalID as globalid,
        latitude as latitude,
        longitude as longitude,
        ST_GEOGFROMTEXT(geometry, make_valid => TRUE) as geometry
    FROM `rj-iplanrio.brutos_equipamentos_staging.unidades_saude_arcgis`
)

SELECT
    *
FROM base