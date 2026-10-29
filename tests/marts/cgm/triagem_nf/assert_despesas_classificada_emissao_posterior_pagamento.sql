{{ config(severity = 'warn') }}

select
    id_documento,
    declaracao_emissao_data,
    declaracao_pagamento_data
from {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
where declaracao_emissao_data is not null
  and declaracao_pagamento_data is not null
  and declaracao_pagamento_data < declaracao_emissao_data