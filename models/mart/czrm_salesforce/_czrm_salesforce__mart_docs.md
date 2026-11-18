{% docs czrm_salesforce__mart__mestre_andamento %}
Histórico de andamentos das ordens de serviço: 1 linha por andamento, para montar a timeline da OS.
{% enddocs %}

{% docs czrm_salesforce__mart__mestre_ordem_servico %}
Tabela mestra de ordens de serviço: 1 linha por OS, com o protocolo desnormalizado, o último andamento e as respostas do formulário agregadas.
{% enddocs %}

{% docs czrm_salesforce__mart__col__andamento_id %}
Chave do andamento no Salesforce (`WorkOrderUpdate__c.Id`, 18 caracteres).
{% enddocs %}

{% docs czrm_salesforce__mart__col__atualizado_em %}
Data/hora da última alteração da OS (`SystemModstamp`), em UTC. Muda a cada edição do registro.
{% enddocs %}

{% docs czrm_salesforce__mart__col__bairro %}
Nome do bairro do local da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__canal_atendimento %}
Canal de atendimento da OS (valor de API de Case.Origin). Gravado na criação: copiado do chamado pelo WorkOrderTrigger (WorkOrderOriginService) quando a OS tem chamado e o campo vem vazio; OS sem chamado grava o próprio canal.
{% enddocs %}

{% docs czrm_salesforce__mart__col__cep %}
CEP do local da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__cidade %}
Município do local da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__codigo_logradouro %}
Código do Logradouro (CL) IPP da via da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__complemento %}
Complemento do endereço da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__cpf %}
CPF do cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__criado_em %}
Data/hora de criação da OS, em UTC. É a coluna de partição da tabela.
{% enddocs %}

{% docs czrm_salesforce__mart__col__criado_por %}
Nome do usuário que criou o registro.
{% enddocs %}

{% docs czrm_salesforce__mart__col__data_hora %}
Data do andamento: `coalesce(data_hora_andamento, created_date)`. Usa a data informada pelo órgão quando existe e a criação do registro quando não. Nunca nula.
{% enddocs %}

{% docs czrm_salesforce__mart__col__descricao %}
Relato do andamento: `coalesce(modified_comments, event_comments)`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__descricao_wo %}
Texto livre com a descrição da solicitação/problema registrado na OS.
{% enddocs %}

{% docs czrm_salesforce__mart__col__dono_nome %}
Nome de quem está com a OS: o usuário ou a fila.
{% enddocs %}

{% docs czrm_salesforce__mart__col__e_equivalencia %}
Indica que esta OS foi agrupada por uma OS agrupadora (é membro de um agrupamento de equivalência). NÃO indica que a OS é agrupadora: para isso ver `qtd_equivalentes > 0`. A agrupadora permanece com este campo false.
{% enddocs %}

{% docs czrm_salesforce__mart__col__email %}
E-mail do cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__encerrada %}
Indica se a ordem de serviço está fechada considerando os status Fechado e Cancelado (o IsClosed nativo não trata Cancelado).
{% enddocs %}

{% docs czrm_salesforce__mart__col__favela %}
Nome da favela/comunidade onde a ocorrência está localizada.
{% enddocs %}

{% docs czrm_salesforce__mart__col__fechada_em %}
Data/hora em que a WorkOrder foi fechada (status Fechado ou Cancelado). Gravado automaticamente no after insert/update. É a fonte da janela pós-fechamento de Ouvidoria (CZRM-2369). Não confundir com WorkOrder.EndDate, que não é populado nesta org.
{% enddocs %}

{% docs czrm_salesforce__mart__col__fila %}
Nome da fila responsável pela OS.
{% enddocs %}

{% docs czrm_salesforce__mart__col__informado_em %}
Data/hora do evento informada pelo órgão (`Data_Hora_Andamento__c`).
{% enddocs %}

{% docs czrm_salesforce__mart__col__latitude %}
Latitude do local da ocorrência, em graus decimais (WGS84).
{% enddocs %}

{% docs czrm_salesforce__mart__col__logradouro %}
Nome do logradouro sem o número.
{% enddocs %}

{% docs czrm_salesforce__mart__col__longitude %}
Longitude do local da ocorrência, em graus decimais (WGS84).
{% enddocs %}

{% docs czrm_salesforce__mart__col__motivo_encerramento %}
Motivo informado no encerramento da OS (ex.: `Fechado com solução`, `Cancelado`, `Não constatado`). Vem vazio na maioria das OSs.
{% enddocs %}

{% docs czrm_salesforce__mart__col__nome_completo %}
Nome completo do cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__numero %}
Número da porta separado do logradouro.
{% enddocs %}

{% docs czrm_salesforce__mart__col__numero_os %}
Número da OS exibido ao cidadão e ao órgão (ex.: `00000009`).
{% enddocs %}

{% docs czrm_salesforce__mart__col__numero_protocolo %}
Número do protocolo (`Case.CaseNumber`) ao qual a OS pertence. Um protocolo pode ter várias OSs.
{% enddocs %}

{% docs czrm_salesforce__mart__col__orgao_central %}
Órgão central responsável pelo serviço
{% enddocs %}

{% docs czrm_salesforce__mart__col__os_id %}
Chave da OS no Salesforce (`WorkOrder.Id`, 18 caracteres). Identifica a linha e serve de join com as demais tabelas.
{% enddocs %}

{% docs czrm_salesforce__mart__col__os_pai_numero %}
Número da OS pai, quando a OS é filha de outra.
{% enddocs %}

{% docs czrm_salesforce__mart__col__precisao_geo %}
Indica se a geolocalização do endereço foi obtida com precisão ROOFTOP.
{% enddocs %}

{% docs czrm_salesforce__mart__col__prioridade %}
Prioridade da OS. Valores normalizados: `Baixa`, `Média`, `Alta`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_assunto %}
Assunto do protocolo.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_criado_em %}
Data/hora de abertura do protocolo, em UTC.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_fechado %}
Verdadeiro quando o protocolo está fechado.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_fechado_em %}
Data/hora de fechamento do protocolo, em UTC.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_origem %}
Canal de origem do protocolo.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_prioridade %}
Prioridade do protocolo.
{% enddocs %}

{% docs czrm_salesforce__mart__col__protocolo_status %}
Status atual do protocolo (`Case.Status`).
{% enddocs %}

{% docs czrm_salesforce__mart__col__publicado_em %}
Data/hora em que o andamento virou oficial (`publication_datetime`). Pode ser posterior à criação; não usar para ordenar a timeline.
{% enddocs %}

{% docs czrm_salesforce__mart__col__qtd_andamentos %}
Quantidade de andamentos registrados para a OS.
{% enddocs %}

{% docs czrm_salesforce__mart__col__qtd_equivalentes %}
Quantidade de WOs equivalentes vinculadas a esta WO agrupadora.
{% enddocs %}

{% docs czrm_salesforce__mart__col__qtd_respostas %}
Quantidade de respostas do formulário de atributos vinculadas à OS.
{% enddocs %}

{% docs czrm_salesforce__mart__col__referencia %}
Ponto de referência do local da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__regiao_administrativa %}
Lookup para o registro de recorte administrativo do tipo RA.
{% enddocs %}

{% docs czrm_salesforce__mart__col__respostas_internas %}
Quantidade de respostas que pertencem à seção interna (Atualização da OS), preenchida pelo despachador e não visível ao cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__respostas_json %}
Respostas do formulário em JSON, no formato `[{"campo": ..., "valor": ...}]`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__sem_numero %}
Indica que o local da ocorrência não possui número.
{% enddocs %}

{% docs czrm_salesforce__mart__col__servico %}
Terceiro nível de classificação (N3), identificando especificamente o serviço que está sendo solicitado.
{% enddocs %}

{% docs czrm_salesforce__mart__col__servico_subtema %}
Segundo nível de classificação (N2), que detalha a área do serviço vinculada ao Tema selecionado.
{% enddocs %}

{% docs czrm_salesforce__mart__col__servico_tema %}
Primeiro nível de classificação (N1), agrupando serviços por grandes áreas temáticas da prefeitura.
{% enddocs %}

{% docs czrm_salesforce__mart__col__situacao %}
Situação do andamento no CRM (`Status__c`): Publicado, Aguardando aprovação, Rejeitado, Cancelado, Rascunho etc. Sempre preenchida; é o que diferencia um andamento publicado de um rascunho.
{% enddocs %}

{% docs czrm_salesforce__mart__col__sla_prazo %}
Data/hora limite para atendimento da OS.
{% enddocs %}

{% docs czrm_salesforce__mart__col__solicitante_nome %}
Nome do solicitante do reverso (m14).
{% enddocs %}

{% docs czrm_salesforce__mart__col__solicitante_telefone %}
Telefone do solicitante informado pelo órgão no reverso (m14).
{% enddocs %}

{% docs czrm_salesforce__mart__col__status %}
Status atual da OS (ex.: `Em andamento`, `Fechado`, `Cancelado`).
{% enddocs %}

{% docs czrm_salesforce__mart__col__subprefeitura %}
Lookup para o registro de recorte administrativo do tipo Subprefeitura.
{% enddocs %}

{% docs czrm_salesforce__mart__col__telefone_1 %}
Telefone principal do cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__telefone_1_tipo %}
Tipo do telefone principal (Celular ou Fixo).
{% enddocs %}

{% docs czrm_salesforce__mart__col__telefone_2 %}
Telefone secundário do cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__telefone_3 %}
Telefone terciário do cidadão.
{% enddocs %}

{% docs czrm_salesforce__mart__col__tipo %}
Tipo do evento de andamento (`Event_Type__c`): `Close_WO`, `Partial_Update`, `Internal_Note`, `Change_Queue_UO`, `Integracao`, `External_Update`, `Transfer_WO`, `Cancel_WO`, `Change_Priority`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__tipo_categoria_servico %}
Indica a categoria do serviço associado a ordem do serviço
{% enddocs %}

{% docs czrm_salesforce__mart__col__tipo_solicitante %}
Tipo do solicitante do chamado. Preenchido no reverso da Defesa (m14); um protocolo pode ter chamados do cidadão e internos/servidor.
{% enddocs %}

{% docs czrm_salesforce__mart__col__titulo %}
Código do andamento (`WorkOrderUpdate__c.Name`, formato `AOS-00000`). Não é título nem resumo — o texto está em `descricao`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__uf %}
Unidade federativa (UF) do local da ocorrência.
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento %}
Código do andamento mais recente (`WorkOrderUpdate__c.Name`, formato `AOS-00000`). Não é título nem resumo: o texto do andamento está em `descricao` do `mestre_andamento`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento_em %}
Data do andamento mais recente: `coalesce(data_hora_andamento, created_date)`. Usa a data informada pelo órgão quando existe e a criação do registro quando não. Nunca nula.
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento_informado_em %}
Data/hora do evento informada pelo órgão (`Data_Hora_Andamento__c`). Nula nos andamentos criados no CRM.
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento_publicado_em %}
Data/hora em que o andamento virou oficial (`publication_datetime`). Pode ser posterior à criação; não usar para ordenar a timeline.
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento_situacao %}
Situação do andamento no CRM (`Status__c`): Publicado, Aguardando aprovação, Rejeitado, Cancelado, Rascunho etc. Sempre preenchida; é o que diferencia um andamento publicado de um rascunho.
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento_status %}
Status do órgão normalizado (`Status_Normalizado__c`). Só existe em andamentos que carregam status do órgão (~18%).
{% enddocs %}

{% docs czrm_salesforce__mart__col__ultimo_andamento_tipo %}
Tipo do evento de andamento (`Event_Type__c`): `Close_WO`, `Partial_Update`, `Internal_Note`, `Change_Queue_UO`, `Integracao`, `External_Update`, `Transfer_WO`, `Cancel_WO`, `Change_Priority`.
{% enddocs %}

{% docs czrm_salesforce__mart__col__uo_nome %}
Nome da unidade organizacional (UO) responsável pela execução.
{% enddocs %}

{% docs czrm_salesforce__mart__col__uo_sigla %}
Nome reduzido da unidade organizacional.
{% enddocs %}

{% docs czrm_salesforce__mart__col__visivel_cidadao %}
Campo fórmula que verifica se a configuração do evento permite visibilidade para o cidadão e se o evento de andamento já encontra-se publicado.
{% enddocs %}

