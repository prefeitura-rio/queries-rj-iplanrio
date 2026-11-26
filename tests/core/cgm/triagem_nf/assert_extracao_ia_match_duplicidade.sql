select
    id_documento,
    ids_grupo_nf,
    rank_declaracao
from {{ ref('core_cgm_triagem_nf__extracao_ia_match') }}
where resultado_extracao_ia = 'NF Encontrada'
  and (
      -- Falha se o ID atual não fizer parte da lista de IDs agrupados ou não tiver mínimo de 2 ids
      not regexp_contains(ids_grupo_nf, concat(r'(^|, )', cast(id_documento as string), r'(,|$)'))
      or
      (ids_grupo_nf not like '%,%' and rank_declaracao > 1)
  )