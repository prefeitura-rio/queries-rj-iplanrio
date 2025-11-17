{{
    config(
        alias="equipamento",
        description="Combinação das tabelas sici.t_ug_atual, sici.ug_tipo_ug, estrutura_batch, tipo_equipamento, horario, endereco e comunicacao para formar a tupla a ser utilizada pelo DataLake	",
            )
}}


select 
    safe_cast(id_equipamento as int64) as id_equipamento,
    safe_cast(Secretaria as string) as secretaria_nome,
    safe_cast(TipoEquipamento as string) as tipo_equipamento,
    safe_cast(nome_oficial as string) as unidade_governamental_nome_oficial,
    safe_cast(nomePopular as string) as equipamento_nome_popular,
    safe_cast(Endereco as json) as equipamento_endereco,
    safe_cast(DS_TIPO_EQUIPAMENTO as string) as equipamento_tipo,
    safe_cast(HorariosJSON as json) as equipamento_horario_atendimento,
    safe_cast(Contato as json) as contato,
    safe_cast(Ativo as string) as ativo,
    safe_cast(aberto_ao_publico as string) as aberto_publico,
    safe_cast(UltimaAtualizacao as date) as ultima_atualizacao,
    safe_cast(Criacao as date) as unidade_governamental_criacao,
    safe_cast(UltimaAtivacao as date) as unidade_governamental_ativacao,
    safe_cast(vigenciaFim as date) as unidade_governamental_fim_vigencia,
    current_timestamp() as datalake_transformed_at
    
from {{ source("brutos_sici_staging", "vw_DataLake") }}

