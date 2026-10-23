-- R-07: notas que o Oracle deixou em CENARIO_EXIGIBILIDADE = -1 (competência
-- municipal >= InicioPlanoB, NSU <= corte), contra int_nc2__cenario, que reavalia
-- contra o estado atual de MV_CONTRIBUINTES sem a janela de 7 dias. v2 ainda em -1 e
-- responsável não habilitado: igual (continua aguardando habilitação); v2 em -1 mas o
-- responsável já está habilitado: divergente (deveria ter sido promovida); v2 fora de
-- -1: correcao_catalogada:B-04 (a promoção, ou a ordem 4, 5 e ISS retido sem exigir
-- habilitação).
with
    oracle as (
        select nota_nacional as chave
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where
            cast(cenario_exigibilidade as int64) = -1
            and nsu <= {{ nc2_corte() }}
            and safe_cast(substr(data_compmunicipio, 1, 10) as date)
            >= (select inicio_plano_b from {{ ref("int_nc2__config") }})
    ),

    v2 as (
        select
            c.nota_nacional as chave, c.cenario_exigibilidade, c.responsavel_habilitado
        from {{ ref("int_nc2__cenario") }} as c
        inner join oracle as o on o.chave = c.nota_nacional
    ),

    comparado as (
        select
            coalesce(v.chave, o.chave) as chave,
            v.chave is not null as no_v2,
            o.chave is not null as no_oracle,
            v.cenario_exigibilidade as cenario_v2,
            v.responsavel_habilitado
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
    )

select
    'R-07' as regra,
    chave,
    case
        when not (no_v2 and no_oracle)
        then 'divergente'
        when cenario_v2 = -1 and responsavel_habilitado
        then 'divergente'
        when cenario_v2 = -1
        then 'igual'
        else 'correcao_catalogada:B-04'
    end as classe,
    concat(
        'oracle=-1 v2=',
        coalesce(cast(cenario_v2 as string), 'ausente'),
        ' habilitado=',
        coalesce(cast(responsavel_habilitado as string), 'ausente')
    ) as detalhe
from comparado
