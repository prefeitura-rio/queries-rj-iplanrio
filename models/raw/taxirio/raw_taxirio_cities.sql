{{
    config(
      schema="brutos_taxirio",
      alias="cidades",
      materialized="table",
      tags=["raw", "taxirio"],
      description="Tabela de Cidades"
    )
}}

SELECT
  safe_cast(id as string) as id_municipio,
  safe_cast(name as string) as nome_municipio,
  safe_cast(stateAbbreviation as string) as abrev_municipio,
  safe_cast(isAbleToUsePaymentInApp as bool) as pode_usar_pagamento_app,
  safe_cast(isCalulatedInApp as bool) as calculado_no_app,
  safe_cast(loginLabel as string) as forma_de_login,
  parse_json(serviceStations) as estacoes_de_servico

FROM
  {{ source('brutos_taxirio_staging', 'cities') }}
