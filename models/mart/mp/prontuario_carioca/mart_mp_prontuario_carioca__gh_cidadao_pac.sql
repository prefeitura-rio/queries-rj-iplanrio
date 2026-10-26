{{
    config(
        alias="gh_cidadao_pac",
        materialized="table",
    )
}}

with
    ciclos as (
        select distinct seqpac
        from {{ source("brutos_acolherio", "gh_pac_ciclos") }}
        where
            -- unidades de teste que não devem ser consideradas
            sequs not in (82, 80, 144, 143, 173, 172, 81, 156, 184, 145, 83, 142)
    ),

    gh_cidadao_pac as (
        select
        SAFE_CAST(seqpac AS INT64) AS seqpac,
        {{ transform_mp("numrg") }} AS numrg,
        {{ transform_mp("dsccns") }} AS dsccns,
        {{ transform_mp("numend") }} AS numend,
        {{ transform_mp("apelido") }} AS apelido,
        {{ transform_mp("dscfoto") }} AS dscfoto,
        {{ transform_mp("indsexo") }} AS indsexo,
        {{ transform_mp("muniend") }} AS muniend,
        {{ transform_mp("racacor") }} AS racacor,
        {{ transform_mp("complend") }} AS complend,
        {{ transform_mp("condestr") }} AS condestr,
        {{ transform_mp("dscemail") }} AS dscemail,
        {{ transform_mp("dscnmmae") }} AS dscnmmae,
        {{ transform_mp("dscnmpai") }} AS dscnmpai,
        {{ transform_mp("dscnotif") }} AS dscnotif,
        {{ transform_mp("estcivil") }} AS estcivil,
        {{ transform_mp("muninasc") }} AS muninasc,
        {{ transform_mp("nacional") }} AS nacional,
        SAFE_CAST(seqetnia AS INT64) AS seqetnia,
        SAFE_CAST(seqlogin AS INT64) AS seqlogin,
        SAFE_CAST(sequsref AS INT64) AS sequsref,
        {{ transform_mp("ufexpedi") }} AS ufexpedi,
        SAFE_CAST(datcadast AS DATE) AS datcadast,
        SAFE_CAST(datexpedi AS DATE) AS datexpedi,
        SAFE_CAST(datnascim AS DATE) AS datnascim,
        {{ transform_mp("dsctelef1") }} AS dsctelef1,
        {{ transform_mp("dsctelef2") }} AS dsctelef2,
        {{ transform_mp("indgenero") }} AS indgenero,
        {{ transform_mp("indstatus") }} AS indstatus,
        {{ transform_mp("numcpfpac") }} AS numcpfpac,
        {{ transform_mp("pontorefe") }} AS pontorefe,
        {{ transform_mp("sigufnasc") }} AS sigufnasc,
        {{ transform_mp("dscnomepac") }} AS dscnomepac,
        {{ transform_mp("dscnomsoci") }} AS dscnomsoci,
        {{ transform_mp("indorigcad") }} AS indorigcad,
        {{ transform_mp("paisorigem") }} AS paisorigem,
        SAFE_CAST(seqequipfm AS INT64) AS seqequipfm,
        {{ transform_mp("sigufender") }} AS sigufender,
        SAFE_CAST(datatualcad AS DATE) AS datatualcad,
        {{ transform_mp("indorgaoexp") }} AS indorgaoexp,
        {{ transform_mp("numcepender") }} AS numcepender,
        SAFE_CAST(sequsambref AS INT64) AS sequsambref,
        SAFE_CAST(sequscapref AS INT64) AS sequscapref,
        {{ transform_mp("dslogradouro") }} AS dslogradouro,
        {{ transform_mp("indusonomsoc") }} AS indusonomsoc,
        {{ transform_mp("nuprontpapel") }} AS nuprontpapel,
        {{ transform_mp("dscbairroender") }} AS dscbairroender,
        {{ transform_mp("dcsnomresplegal") }} AS dcsnomresplegal,
        {{ transform_mp("dscmodatendativ") }} AS dscmodatendativ,
        {{ transform_mp("numcpfresplegal") }} AS numcpfresplegal,
        SAFE_CAST(seqprofrefdamb1 AS INT64) AS seqprofrefdamb1,
        SAFE_CAST(seqprofrefdamb2 AS INT64) AS seqprofrefdamb2,
        {{ transform_mp("dscvincresplegal") }} AS dscvincresplegal,
        {{ transform_mp("dsctelefresplegal") }} AS dsctelefresplegal,
        SAFE_CAST(seqprofequirefer1 AS INT64) AS seqprofequirefer1,
        SAFE_CAST(seqprofequirefer2 AS INT64) AS seqprofequirefer2
        from {{ source("brutos_acolherio", "gh_cidadao_pac") }}
        inner join ciclos using (seqpac)
        where dscnomepac not like '%TESTE%'
    )

select *
from gh_cidadao_pac
