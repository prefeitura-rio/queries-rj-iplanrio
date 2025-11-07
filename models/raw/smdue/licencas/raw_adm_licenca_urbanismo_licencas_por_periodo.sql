{{
    config(
        alias="view_lic_licencas_por_periodo",
        description="Dados brutos de licenças por período (origem vwLic_LicencasPorPeriodo no banco SMU_PRD)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
select
  -- Requerente
  safe_cast(requerente as string) as requerente,
  
  -- Data de emissão
  safe_cast(Dt_emissao as date) as data_emissao,
  
  -- Identificadores da licença
  safe_cast(num_lic as int64) as identificador_licenca,
  safe_cast(classe_licenca as string) as classe_licenca,
  safe_cast(licenca as string) as numero_licenca,
  safe_cast(num_proc as string) as numero_processo,
  
  -- Endereço
  safe_cast(endereco as string) as endereco,
  safe_cast(codlogra as int64) as codigo_logradouro,
  safe_cast(tipo as string) as tipo_logradouro,
  safe_cast(nobreza as string) as nobreza,
  safe_cast(preposicao as string) as preposicao,
  safe_cast(nomelogra as string) as nome_logradouro,
  safe_cast(num as string) as numero_porta,
  safe_cast(complemento as string) as complemento_endereco,
  
  -- Projeto e localização
  safe_cast(pal as string) as projeto_alinhamento,
  safe_cast(quadra as string) as quadra,
  safe_cast(lote as string) as lote,
  safe_cast(codap as int64) as codigo_ap,
  safe_cast(nombairro as string) as nome_bairro,
  
  -- Informações da licença
  safe_cast(licgratis as int64) as licenca_gratis,
  
  -- Características da edificação
  safe_cast(area as float64) as area_total_edificada,
  safe_cast(Tipologia as string) as tipologia_edificacao,
  safe_cast(Uso as string) as uso_edificacao,
  safe_cast(qtd_edificaoes as int64) as quantidade_edificaoes_licen,
  safe_cast(total_unidades as int64) as total_unidades_edificacao,
  safe_cast(total_residencial as int64) as total_unidades_residencial,
  safe_cast(total_lojas as int64) as total_unidades_loja,
  safe_cast(total_salas as int64) as total_unidade_sala,
  
  -- Auditoria de alteração
  safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
from {{ source("brutos_adm_licenca_urbanismo_staging", "vwLic_LicencasPorPeriodo") }}

