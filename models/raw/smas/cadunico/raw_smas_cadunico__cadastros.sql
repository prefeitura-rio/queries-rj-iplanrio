with source as (
        select * from {{ source('rj-smas', 'cadastros') }}
  ),
  renamed as (
      select
          {{ adapter.quote("cpf") }},
        {{ adapter.quote("id_membro_familia") }},
        {{ adapter.quote("id_familia") }},
        {{ adapter.quote("data_particao") }},
        {{ adapter.quote("dados") }},
        {{ adapter.quote("deficiencia") }},
        {{ adapter.quote("escolaridade") }},
        {{ adapter.quote("renda") }},
        {{ adapter.quote("endereco") }},
        {{ adapter.quote("domicilio") }},
        {{ adapter.quote("membros") }},
        {{ adapter.quote("cpf_particao") }}

      from source
  )
  select * from renamed
    