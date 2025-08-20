with source as (
        select * from {{ source('rj-crm-registy-intermediario-dados-mestres-staging', 'enderecos_geolocalizados') }}
  ),
  renamed as (
      select
          {{ adapter.quote("logradouro_tratado") }},
        {{ adapter.quote("numero_porta") }},
        {{ adapter.quote("bairro") }},
        {{ adapter.quote("endereco_completo") }},
        {{ adapter.quote("latitude") }},
        {{ adapter.quote("longitude") }},
        {{ adapter.quote("logradouro_geocode") }},
        {{ adapter.quote("numero_porta_geocode") }},
        {{ adapter.quote("bairro_geocode") }},
        {{ adapter.quote("cidade_geocode") }},
        {{ adapter.quote("estado_geocode") }},
        {{ adapter.quote("cep_geocode") }},
        {{ adapter.quote("confianca") }},
        {{ adapter.quote("updated_date") }},
        {{ adapter.quote("geocode") }},
        {{ adapter.quote("pluscode") }}

      from source
  )
  select * from renamed
    