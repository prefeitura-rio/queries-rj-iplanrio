with source as (
        select * from {{ source('rj-smtr', 'transacao_cpf') }}
  ),
  renamed as (
      select
          {{ adapter.quote("cpf_particao") }},
        {{ adapter.quote("cpf_cliente") }},
        {{ adapter.quote("data") }},
        {{ adapter.quote("hora") }},
        {{ adapter.quote("datetime_transacao") }},
        {{ adapter.quote("datetime_processamento") }},
        {{ adapter.quote("datetime_captura") }},
        {{ adapter.quote("modo") }},
        {{ adapter.quote("id_consorcio") }},
        {{ adapter.quote("consorcio") }},
        {{ adapter.quote("id_operadora") }},
        {{ adapter.quote("operadora") }},
        {{ adapter.quote("id_servico_jae") }},
        {{ adapter.quote("servico_jae") }},
        {{ adapter.quote("descricao_servico_jae") }},
        {{ adapter.quote("sentido") }},
        {{ adapter.quote("id_veiculo") }},
        {{ adapter.quote("id_validador") }},
        {{ adapter.quote("id_transacao") }},
        {{ adapter.quote("tipo_pagamento") }},
        {{ adapter.quote("tipo_transacao") }},
        {{ adapter.quote("tipo_transacao_smtr") }},
        {{ adapter.quote("tipo_gratuidade") }},
        {{ adapter.quote("latitude") }},
        {{ adapter.quote("longitude") }},
        {{ adapter.quote("geo_point_transacao") }},
        {{ adapter.quote("valor_transacao") }},
        {{ adapter.quote("versao") }},
        {{ adapter.quote("datetime_ultima_atualizacao") }}

      from source
  )
  select * from renamed
    