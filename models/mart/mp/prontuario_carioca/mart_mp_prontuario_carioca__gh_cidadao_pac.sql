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
        SAFE_CAST(numrg AS STRING) AS numrg,
        SAFE_CAST(dsccns AS STRING) AS dsccns,
        SAFE_CAST(numend AS STRING) AS numend,
        SAFE_CAST(apelido AS STRING) AS apelido,
        SAFE_CAST(dscfoto AS STRING) AS dscfoto,
        SAFE_CAST(indsexo AS STRING) AS indsexo,
        SAFE_CAST(muniend AS STRING) AS muniend,
        SAFE_CAST(racacor AS STRING) AS racacor,
        SAFE_CAST(complend AS STRING) AS complend,
        SAFE_CAST(condestr AS STRING) AS condestr,
        SAFE_CAST(dscemail AS STRING) AS dscemail,
        SAFE_CAST(dscnmmae AS STRING) AS dscnmmae,
        SAFE_CAST(dscnmpai AS STRING) AS dscnmpai,
        SAFE_CAST(dscnotif AS STRING) AS dscnotif,
        SAFE_CAST(estcivil AS STRING) AS estcivil,
        SAFE_CAST(muninasc AS STRING) AS muninasc,
        SAFE_CAST(nacional AS STRING) AS nacional,
        SAFE_CAST(seqetnia AS INT64) AS seqetnia,
        SAFE_CAST(seqlogin AS INT64) AS seqlogin,
        SAFE_CAST(sequsref AS INT64) AS sequsref,
        SAFE_CAST(ufexpedi AS STRING) AS ufexpedi,
        SAFE_CAST(datcadast AS DATE) AS datcadast,
        SAFE_CAST(datexpedi AS DATE) AS datexpedi,
        SAFE_CAST(datnascim AS DATE) AS datnascim,
        SAFE_CAST(dsctelef1 AS STRING) AS dsctelef1,
        SAFE_CAST(dsctelef2 AS STRING) AS dsctelef2,
        SAFE_CAST(indgenero AS STRING) AS indgenero,
        SAFE_CAST(indstatus AS STRING) AS indstatus,
        SAFE_CAST(numcpfpac AS STRING) AS numcpfpac,
        SAFE_CAST(pontorefe AS STRING) AS pontorefe,
        SAFE_CAST(sigufnasc AS STRING) AS sigufnasc,
        SAFE_CAST(dscnomepac AS STRING) AS dscnomepac,
        SAFE_CAST(dscnomsoci AS STRING) AS dscnomsoci,
        SAFE_CAST(indorigcad AS STRING) AS indorigcad,
        SAFE_CAST(paisorigem AS STRING) AS paisorigem,
        SAFE_CAST(seqequipfm AS INT64) AS seqequipfm,
        SAFE_CAST(sigufender AS STRING) AS sigufender,
        SAFE_CAST(datatualcad AS DATE) AS datatualcad,
        SAFE_CAST(indorgaoexp AS STRING) AS indorgaoexp,
        SAFE_CAST(numcepender AS STRING) AS numcepender,
        SAFE_CAST(sequsambref AS INT64) AS sequsambref,
        SAFE_CAST(sequscapref AS INT64) AS sequscapref,
        SAFE_CAST(dslogradouro AS STRING) AS dslogradouro,
        SAFE_CAST(indusonomsoc AS STRING) AS indusonomsoc,
        SAFE_CAST(nuprontpapel AS STRING) AS nuprontpapel,
        SAFE_CAST(dscbairroender AS STRING) AS dscbairroender,
        SAFE_CAST(dcsnomresplegal AS STRING) AS dcsnomresplegal,
        SAFE_CAST(dscmodatendativ AS STRING) AS dscmodatendativ,
        SAFE_CAST(numcpfresplegal AS STRING) AS numcpfresplegal,
        SAFE_CAST(seqprofrefdamb1 AS INT64) AS seqprofrefdamb1,
        SAFE_CAST(seqprofrefdamb2 AS INT64) AS seqprofrefdamb2,
        SAFE_CAST(dscvincresplegal AS STRING) AS dscvincresplegal,
        SAFE_CAST(dsctelefresplegal AS STRING) AS dsctelefresplegal,
        SAFE_CAST(seqprofequirefer1 AS INT64) AS seqprofequirefer1,
        SAFE_CAST(seqprofequirefer2 AS INT64) AS seqprofequirefer2
        from {{ source("brutos_acolherio", "gh_cidadao_pac") }}
        inner join ciclos using (seqpac)
        where dscnomepac not like '%TESTE%'
    )

select *
from gh_cidadao_pac
