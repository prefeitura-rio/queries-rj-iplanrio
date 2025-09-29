{{
    config(
        alias="pessoa_vinculada",
        description="Dados brutos de pessoas vinculadas ao sistema, contendo informações pessoais, funcionais e de lotação",
            )
}}

select 
    safe_cast(id as int64) as identificador_pessoa_vinculada,
    safe_cast(obs as string) as observacao_registro,
    safe_cast(status as string) as status_vinculacao,
    safe_cast(matricula as string) as matricula,
    safe_cast(nome as string) as nome,
    safe_cast(cpf as string) as cpf,
    safe_cast(dt_nascimento as date) as data_nascimento,
    safe_cast(dt_inclusao as datetime) as data_inclusao,
    safe_cast(dt_atualizacao as datetime) as data_atualizacao,
    safe_cast(regime_juridico as string) as regime_juridico,
    safe_cast(prefixo_matricula as int64) as prefixo_matricula,
    safe_cast(email_alternativo as string) as email_alternativo,
    safe_cast(email_institucional as string) as email_institucional,
    safe_cast(e_ficticia as string) as matricula_ficticia,
    safe_cast(CARGO as string) as cargo,
    safe_cast(FUNCAO as string) as funcao,
    safe_cast(DT_FUNCAO as datetime) as data_funcao,
    safe_cast(cd_lotacao as int64) as identificador_lotacao, 
    safe_cast(ua2.nome_unidade_administrativa as string) as nome_unidade_administrativa,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
from {{ source("brutos_pessoa_vinculada_staging", "vw_cidadao_dataLake") }}  l
left join {{ ref("raw_unidade_administrativa") }} ua on l.cd_lotacao = ua.id_unidade_administrativa and ua.ativa = 'S'
left join {{ ref("raw_unidade_administrativa") }} ua2 on ua.id_unidade_administrativa_basica = ua2.id_unidade_administrativa and ua2.ativa = 'S'

      