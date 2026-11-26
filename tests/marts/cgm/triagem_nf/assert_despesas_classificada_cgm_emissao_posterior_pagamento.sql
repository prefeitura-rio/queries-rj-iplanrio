{{ config(severity="warn") }}

select id_documento_osinfo, declaracao_emissao_data_osinfo, declaracao_pagamento_data
from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
where
    declaracao_emissao_data_osinfo is not null
    and declaracao_pagamento_data is not null
    and declaracao_pagamento_data < declaracao_emissao_data_osinfo
