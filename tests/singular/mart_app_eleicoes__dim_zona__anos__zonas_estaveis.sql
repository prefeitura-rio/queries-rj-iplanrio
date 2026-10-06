-- retorna zonas que não aparecem nos três anos do recorte (2022, 2024 e 2026).
-- uma zona nova ou extinta quebra a comparação direta entre eleições pelo id_zona.
{{
    config(
        alias="mart_app_eleicoes__dim_zona__anos__zonas_estaveis",
        severity="warn",
    )
}}

select id_zona, anos
from {{ ref("mart_app_eleicoes__dim_zona") }}
where array_length(anos) < 3
