{{
    config(
        alias="subprefeitura",
        description="Dados de subprefeituras do município do Rio de Janeiro",
    )
}}

select
    safe_cast(trim(subprefeitura) as string) as subprefeitura,
    safe_cast(regexp_replace(shape_area, r',', '.') as float64) as area,
    safe_cast(regexp_replace(shape_length, r',', '.') as float64) as perimetro,
    safe_cast(geometry as string) geometry_wkt,
    st_geogfromtext(geometry, make_valid => true) as geometria,
from {{ source("brutos_dados_mestres_staging", "subprefeitura") }} as t