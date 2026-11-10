{{
    config(
        schema="projeto_bi_dtgg",
        alias='dimensao_setor',
        materialized="table",
        tags=["mart", "projeto_bi_dtgg", "recursos_humanos", "dtgg", "setores"],
        description="Tabela que contém a dimensão de setores, com relacionamento hierárquico, de modo que é possível estabelecer relação de subordinação e o caminho até chegar num setor específico."
    )
}}

with recursive setores_iplan as (
  select concat(repeat("0", 15 - length(raiz.id_setor)), raiz.id_setor) as ordem,
    to_hex(md5(concat(repeat("0", 15 - length(raiz.id_setor)), raiz.id_setor, SPLIT(raiz.sigla, '/')[OFFSET(ARRAY_LENGTH(SPLIT(raiz.sigla, '/')) - 1)]))) as id_setor_hash,
    raiz.id_setor as caminho_id_setor,
    SPLIT(raiz.sigla, '/')[OFFSET(ARRAY_LENGTH(SPLIT(raiz.sigla, '/')) - 1)] as caminho_sigla,
    raiz.id_setor,
    raiz.id_empresa,
    raiz.id_secretaria,
    raiz.id_setor_pai,
    safe_cast(null as string) as id_setor_hash_pai,
    raiz.sigla as sigla_cadastro, 
    SPLIT(raiz.sigla, '/')[OFFSET(ARRAY_LENGTH(SPLIT(raiz.sigla, '/')) - 1)] as sigla_individual,
    raiz.nome_completo,
    raiz.data_inicio,
    raiz.data_fim, 
    1 as id_nivel, 
    extinto
  from {{ ref("raw_ergon_iplanrio__setor") }} raiz
  where id_setor in ('1153') --, '1451', '5651') - comentado, pois apenas o código 1153 será considerado para a IplanRio
  and ifnull(data_fim, current_date("America/Sao_Paulo")) between '2025-01-01' and '9999-12-31' -- Definindo histórico de registros a partir de 2025.

  union all

  select concat(ordem, repeat("0", 15 - length(ramos.id_setor)), ramos.id_setor), 
    to_hex(md5(concat(ordem, repeat("0", 15 - length(ramos.id_setor)), ramos.id_setor, concat(setores_iplan.caminho_sigla, "/", SPLIT(ramos.sigla, '/')[OFFSET(ARRAY_LENGTH(SPLIT(ramos.sigla, '/')) - 1)])))),
    concat(setores_iplan.caminho_id_setor, "->", ramos.id_setor),
    concat(setores_iplan.caminho_sigla, "/", SPLIT(ramos.sigla, '/')[OFFSET(ARRAY_LENGTH(SPLIT(ramos.sigla, '/')) - 1)]),
    ramos.id_setor,
    ramos.id_empresa,
    ramos.id_secretaria,
    ramos.id_setor_pai,
    setores_iplan.id_setor_hash,
    ramos.sigla, 
    SPLIT(ramos.sigla, '/')[OFFSET(ARRAY_LENGTH(SPLIT(ramos.sigla, '/')) - 1)],
    ramos.nome_completo,
    ramos.data_inicio,
    ramos.data_fim,
    setores_iplan.id_nivel + 1,
    ramos.extinto 
  from {{ ref("raw_ergon_iplanrio__setor") }} ramos
  inner join setores_iplan 
    on ramos.id_setor_pai = setores_iplan.id_setor
    and ramos.id_empresa = setores_iplan.id_empresa
    and ramos.data_inicio between setores_iplan.data_inicio and ifnull(setores_iplan.data_fim, current_date("America/Sao_Paulo"))
)

select distinct ordem, 
  id_setor_hash,
  id_setor_hash_pai,
  caminho_id_setor,
  caminho_sigla,
  id_setor, 
  id_setor_pai, 
  id_empresa, 
  id_secretaria, 
  sigla_cadastro,
  sigla_individual,
  nome_completo,
  id_nivel,
  SPLIT(caminho_sigla, '/')[safe_offset(0)] as sigla_nivel_1, 
  SPLIT(caminho_sigla, '/')[safe_offset(1)] as sigla_nivel_2, 
  SPLIT(caminho_sigla, '/')[safe_offset(2)] as sigla_nivel_3, 
  SPLIT(caminho_sigla, '/')[safe_offset(3)] as sigla_nivel_4, 
  SPLIT(caminho_sigla, '/')[safe_offset(4)] as sigla_nivel_5
from setores_iplan 
order by ordem
