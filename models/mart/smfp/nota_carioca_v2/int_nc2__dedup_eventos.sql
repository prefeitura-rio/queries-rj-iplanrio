{{ config(cluster_by=["chave_acesso"]) }}

-- R-04 para eventos: EVENTO (o Id do evento) repetido mantém o menor NSU
-- (Eventos.vb:926); no mesmo NSU, a linha mais recentemente ingerida. Evento sem Id
-- não tem como duplicar e fica como está.
select evento, chave_acesso, tipo_evento, data_evento, chave_acesso_substituta, nsu
from {{ nc2_raw("eventos") }}
qualify
    row_number() over (
        partition by coalesce(evento, concat('nsu:', cast(nsu as string)))
        order by nsu asc, ingerido_em desc
    )
    = 1
