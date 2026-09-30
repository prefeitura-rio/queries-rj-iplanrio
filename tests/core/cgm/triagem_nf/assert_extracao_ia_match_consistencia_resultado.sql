select
    id_documento,
    resultado_extracao_ia,
    pagina_nf_extracao_ia,
    cnpj_cpf_extracao_ia,
    valor_documento_extracao_ia
from {{ ref('core_cgm_triagem_nf__extracao_ia_match') }}
where
    -- Declarado como NF Encontrada, mas campos essenciais estão nulos
    (resultado_extracao_ia = 'NF Encontrada' and (
        pagina_nf_extracao_ia is null
        or cnpj_cpf_extracao_ia is null
        or valor_documento_extracao_ia is null
        or ids_grupo_nf is null
    ))
    or
    -- Sem match ou não encontrada, mas com dados de extração preenchidos
    (resultado_extracao_ia in ('NF Sem Match', 'NF Não Encontrada') and (
        pagina_nf_extracao_ia is not null
        or cnpj_cpf_extracao_ia is not null
        or valor_documento_extracao_ia is not null
    ))