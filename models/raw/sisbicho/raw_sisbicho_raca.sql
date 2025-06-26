{{
    config(
      schema="brutos_sisbicho",
      alias="raca",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Raças de Animais"
    )
}}

select
    safe_cast(IDRaca as integer) as id_raca,
    safe_cast(NomeRaca as string) as raca_nome,
    safe_cast(IDEspecie as smallint) as id_especie,
    safe_cast(IDTipoEspecie as smallint) as id_tipo_especie
FROM {{ source('brutos_sisbicho_staging', 'Raca') }} 