{{
    config(
      schema="brutos_sisbicho",
      alias="raca",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Raças de Animais"
    )
}}

SELECT 
    SAFE_cast(IDRaca as integer) as id_raca,
    SAFE_cast(NomeRaca as string) as raca_nome,
    SAFE_cast(IDEspecie as smallint) as id_especie,
    SAFE_cast(IDTipoEspecie as smallint) as id_tipo_especie
FROM {{ source('brutos_sisbicho_staging', 'Raca') }} 