{{
    config(
        materialized="table",
        schema="intermediario_rmi_conversas"
    )
}}

-- Status de entrega das HSMs com tracking disponível
-- União das tabelas mensais de fluxo_atendimento

select 
    triggerId as id_hsm,
    flatTarget as telefone_contato,
    
    -- Timestamps com ajuste de fuso horário
    timestamp_sub(sendDate, interval 3 hour) as envio_datahora,
    timestamp_sub(deliveryDate, interval 3 hour) as entrega_datahora,
    timestamp_sub(readDate, interval 3 hour) as leitura_datahora,
    timestamp_sub(replyDate, interval 3 hour) as resposta_datahora,
    timestamp_sub(failedDate, interval 3 hour) as falha_datahora,
    
    faultDescription as descricao_falha,
    status as status_entrega
    
from (
    select triggerId, flatTarget, sendDate, deliveryDate, readDate, replyDate, failedDate, faultDescription, status
    from `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
)
where sendDate >= '2020-01-01'
    and triggerId is not null