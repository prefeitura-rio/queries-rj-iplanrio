{% docs czrm_salesforce__airbyte_extracted_at %}
Data/hora da extração no Airbyte (controle de carga).
{% enddocs %}

{% docs czrm_salesforce__account %}
Tabela raw do objeto Salesforce correspondente a account.
{% enddocs %}

{% docs czrm_salesforce__contact %}
Tabela raw do objeto Salesforce correspondente a contact.
{% enddocs %}

{% docs czrm_salesforce__individual %}
Tabela raw do objeto Salesforce correspondente a individual.
{% enddocs %}

{% docs czrm_salesforce__neighborhood %}
Tabela raw do objeto Salesforce correspondente a neighborhood.
{% enddocs %}

{% docs czrm_salesforce__organizational_unity %}
Tabela raw do objeto Salesforce correspondente a organizational_unity.
{% enddocs %}

{% docs czrm_salesforce__queue_group %}
Tabela raw do objeto Salesforce correspondente a queue_group.
{% enddocs %}

{% docs czrm_salesforce__record_type %}
Tabela raw do objeto Salesforce correspondente a record_type.
{% enddocs %}

{% docs czrm_salesforce__service_attribute_library %}
Tabela raw do objeto Salesforce correspondente a service_attribute_library.
{% enddocs %}

{% docs czrm_salesforce__service_case %}
Tabela raw do objeto Salesforce correspondente a service_case.
{% enddocs %}

{% docs czrm_salesforce__service_catalog %}
Tabela raw do objeto Salesforce correspondente a service_catalog.
{% enddocs %}

{% docs czrm_salesforce__user %}
Tabela raw do objeto Salesforce correspondente a user.
{% enddocs %}

{% docs czrm_salesforce__work_order %}
Tabela raw do objeto Salesforce correspondente a work_order.
{% enddocs %}

{% docs czrm_salesforce__work_order_attribute_answer %}
Tabela raw do objeto Salesforce correspondente a work_order_attribute_answer.
{% enddocs %}

{% docs czrm_salesforce__work_order_relationship %}
Tabela raw do objeto Salesforce correspondente a work_order_relationship.
{% enddocs %}

{% docs czrm_salesforce__work_order_update %}
Tabela raw do objeto Salesforce correspondente a work_order_update.
{% enddocs %}

{% docs czrm_salesforce__col__about_me %}
Sobre mim
{% enddocs %}

{% docs czrm_salesforce__col__account_id %}
ID da conta
{% enddocs %}

{% docs czrm_salesforce__col__account_number %}
Número da conta
{% enddocs %}

{% docs czrm_salesforce__col__account_source %}
Origem da conta
{% enddocs %}

{% docs czrm_salesforce__col__account_type %}
Tipo de conta
{% enddocs %}

{% docs czrm_salesforce__col__address %}
Endereço
{% enddocs %}

{% docs czrm_salesforce__col__address_complement %}
Complemento do endereco do cidadao (apto, bloco, sala, etc.). Armazenado separadamente do campo de endereco padrao do Salesforce.
{% enddocs %}

{% docs czrm_salesforce__col__address_enrichment_at %}
Momento em que o enriquecimento de endereco via IPP foi TENTADO (sucesso, divergencia ou falha). O gate do m1 usa este carimbo para nao esperar indefinidamente por um enriquecimento que nao vem mais.
{% enddocs %}

{% docs czrm_salesforce__col__address_type %}
Tipo do endereco da ocorrencia: Casa, Condominio ou Vila.
{% enddocs %}

{% docs czrm_salesforce__col__alias %}
Apelido
{% enddocs %}

{% docs czrm_salesforce__col__allow_citizen_visibility %}
Campo flag preenchido via flow no momento da criação do registro de andamento da OS e que replica a configuração de visibilidade definida no objeto.
{% enddocs %}

{% docs czrm_salesforce__col__annual_revenue %}
Receita anual
{% enddocs %}

{% docs czrm_salesforce__col__answered_at %}
Data e hora em que a resposta foi registrada na Ordem de Serviço.
{% enddocs %}

{% docs czrm_salesforce__col__ap %}
Lookup para o registro de recorte administrativo do tipo AP
{% enddocs %}

{% docs czrm_salesforce__col__approval_outcome %}
Registra o resultado do processo de aprovação do andamento. Preenchido pelo screen flow de aprovação. Usado em métricas de intervenção do Despachador Central.
{% enddocs %}

{% docs czrm_salesforce__col__approval_process_start %}
Campo preenchido via flow sempre que é iniciado um processo de aprovação para o registro de andamento.
{% enddocs %}

{% docs czrm_salesforce__col__approval_queue_id %}
ID do Group (Queue) de aprovação desta UO (UO_<SHORTNAME>_APPROVAL). Preenchido pelo SyncUOSetup (criação da UO) ou pelo QueueMembershipService.ApprovalQueueJob. Usado pelos fluxos de aprovação para resolver o aprovador dos Andamentos da OS.
{% enddocs %}

{% docs czrm_salesforce__col__approve_decision_datetime %}
Campo preenchido via flow sempre que é finalizado um processo de aprovação para o registro de andamento
{% enddocs %}

{% docs czrm_salesforce__col__approver %}
Caso o evento tenha passado por um processo de aprovação é armazenado neste campo o usuário que aprovou ou rejeitou o registro de andamento.
{% enddocs %}

{% docs czrm_salesforce__col__aps %}
Lookup para o registro de recorte administrativo do tipo APS (Área Programática)
{% enddocs %}

{% docs czrm_salesforce__col__asset_id %}
ID do ativo
{% enddocs %}

{% docs czrm_salesforce__col__assigned_uo %}
Unidade Organizacional responsável pelo atendimento desta Work Order.
{% enddocs %}

{% docs czrm_salesforce__col__assinatura_case_owner %}
Campo que retorna o nome do usuário no formato \"Primeiro nome + primeira letra do sobrenome\", para utilização em assinatura básica de e-mail.
{% enddocs %}

{% docs czrm_salesforce__col__assistant_phone %}
Telefone do assistente
{% enddocs %}

{% docs czrm_salesforce__col__assunto_tratado %}
Trata o campo assunto do e-mail para remover a marcação de possível SPAM
{% enddocs %}

{% docs czrm_salesforce__col__ativo %}
Indica se a UO existe no organograma ativo. Se desmarcado impede roteamento novo
{% enddocs %}

{% docs czrm_salesforce__col__attribute_label %}
Rótulo do atributo exibido ao cidadão/despachador no momento da resposta. Congelado aqui para preservar o histórico e permitir relatório sem depender da configuração atual.
{% enddocs %}

{% docs czrm_salesforce__col__attribute_order %}
Ordem de exibição do atributo no formulário no momento da resposta. Permite reconstruir a sequência original das perguntas.
{% enddocs %}

{% docs czrm_salesforce__col__attribute_type %}
Tipo do atributo (Text, TextArea, Number, Date, DateTime, Picklist, Multipicklist, Checkbox, Phone, Email, Endereco, Customizado) no momento da resposta. Define qual campo de valor está preenchido nesta linha.
{% enddocs %}

{% docs czrm_salesforce__col__available_for_citizen %}
Define se o serviço ficará visível para solicitação direta nos canais de atendimento ao cidadão.
{% enddocs %}

{% docs czrm_salesforce__col__badge_text %}
Texto sobreposto no emblema da foto do usuário
{% enddocs %}

{% docs czrm_salesforce__col__banner_photo_url %}
URL da foto de banner
{% enddocs %}

{% docs czrm_salesforce__col__billing_address %}
Endereço de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_city %}
Cidade de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_country %}
País de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_geocode_accuracy %}
Precisão do geocódigo de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_latitude %}
Latitude de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_longitude %}
Longitude de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_postal_code %}
CEP de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_state %}
Estado/Província de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__billing_street %}
Logradouro de cobrança
{% enddocs %}

{% docs czrm_salesforce__col__birth_date %}
Data de nascimento
{% enddocs %}

{% docs czrm_salesforce__col__birthdate %}
Data de nascimento
{% enddocs %}

{% docs czrm_salesforce__col__business_hours_id %}
ID do horário comercial
{% enddocs %}

{% docs czrm_salesforce__col__business_process_id %}
ID do processo de negócio
{% enddocs %}

{% docs czrm_salesforce__col__buyer_attributes %}
Atributos do comprador
{% enddocs %}

{% docs czrm_salesforce__col__bypass_validation_rule %}
Campo técnico que permite que alguns flows executem sem esbarrar em regras de validação criadas para impedir modificações manais por usuários
{% enddocs %}

{% docs czrm_salesforce__col__call_center_id %}
ID do call center
{% enddocs %}

{% docs czrm_salesforce__col__can_store_pii_elsewhere %}
Permitido armazenar dados pessoais em outros locais
{% enddocs %}

{% docs czrm_salesforce__col__canal_atendimento %}
Canal de atendimento da OS (valor de API de Case.Origin). Gravado na criação: copiado do chamado pelo WorkOrderTrigger (WorkOrderOriginService) quando a OS tem chamado e o campo vem vazio; OS sem chamado grava o próprio canal. Substitui a leitura da fórmula Origin__c, que continua existindo.
{% enddocs %}

{% docs czrm_salesforce__col__canal_preferencial %}
Canal de atendimento preferido do próprio usuário no wizard Novo Protocolo (beta). Guarda o valor de API de Case.Origin de um dos canais manuais (ProtocolService.MANUAL_ORIGINS); valor fora da lista é ignorado na leitura. Gravado só pelo próprio usuário (ProtocolWizardController.savePreferredOrigin).
{% enddocs %}

{% docs czrm_salesforce__col__cancellation_datetime %}
Data e hora em que o andamento foi cancelado. Preenchido automaticamente quando o status passa para Cancelado.
{% enddocs %}

{% docs czrm_salesforce__col__cancellation_reason %}
Motivo do cancelamento do andamento. Preenchido automaticamente pelo cancelamento automático (fechamento/cancelamento da OS) ou pelo cancelamento manual do andamento. Exibe valores do GVS = WorkOrderUpdate_Cancellation_Reason
{% enddocs %}

{% docs czrm_salesforce__col__case_id %}
ID do protocolo
{% enddocs %}

{% docs czrm_salesforce__col__case_number %}
Número do protocolo
{% enddocs %}

{% docs czrm_salesforce__col__case_type %}
Tipo do protocolo
{% enddocs %}

{% docs czrm_salesforce__col__categoria %}
Campo N1 para catalogação do protocolo
{% enddocs %}

{% docs czrm_salesforce__col__chat_fora_do_horario_de_atendimento %}
Campo que registra caso o protocolo gerado seja a partir de uma tentativa de contato no chat enquanto o time de atendimento estava indisponível.
{% enddocs %}

{% docs czrm_salesforce__col__children_count %}
Número de filhos
{% enddocs %}

{% docs czrm_salesforce__col__city %}
Cidade
{% enddocs %}

{% docs czrm_salesforce__col__close_reason %}
Motivo de fechamento
{% enddocs %}

{% docs czrm_salesforce__col__closed_date %}
Data de fechamento
{% enddocs %}

{% docs czrm_salesforce__col__codbairro %}
Código com zero à esquerda (ex: \"013\"). External ID para upsert.
{% enddocs %}

{% docs czrm_salesforce__col__codbnum %}
Código inteiro. Chave de join com cod_bairro do Logradouro__c.
{% enddocs %}

{% docs czrm_salesforce__col__comentarios_de_solucao %}
Campo para que o usuário informe detalhes sobre como foi o atendimento
{% enddocs %}

{% docs czrm_salesforce__col__comments %}
Comentários internos do protocolo.
{% enddocs %}

{% docs czrm_salesforce__col__community_nickname %}
Apelido
{% enddocs %}

{% docs czrm_salesforce__col__company_name %}
Nome da empresa do usuário.
{% enddocs %}

{% docs czrm_salesforce__col__complement %}
Complemento do endereco da ocorrencia (apto, bloco, sala, casa, etc.).
{% enddocs %}

{% docs czrm_salesforce__col__consumer_credit_score %}
Pontuação de crédito do consumidor
{% enddocs %}

{% docs czrm_salesforce__col__consumer_credit_score_provider_name %}
Nome do provedor de pontuação de crédito
{% enddocs %}

{% docs czrm_salesforce__col__contact_email %}
E-mail do contato
{% enddocs %}

{% docs czrm_salesforce__col__contact_fax %}
Fax do contato
{% enddocs %}

{% docs czrm_salesforce__col__contact_id %}
ID do contato
{% enddocs %}

{% docs czrm_salesforce__col__contact_mobile %}
Celular do contato
{% enddocs %}

{% docs czrm_salesforce__col__contact_phone %}
Telefone do contato
{% enddocs %}

{% docs czrm_salesforce__col__contact_source %}
Origem de criação
{% enddocs %}

{% docs czrm_salesforce__col__content_approval_queue_id %}
ID do Group (Queue) de aprovacao de conteudo desta UO (UO_<SHORTNAME>_APV_CONTENT). Preenchido somente em UOs do tipo Orgao (N1).
{% enddocs %}

{% docs czrm_salesforce__col__convictions_count %}
Número de condenações
{% enddocs %}

{% docs czrm_salesforce__col__country %}
País
{% enddocs %}

{% docs czrm_salesforce__col__coverage %}
Aponta para o registro de abrangência da UO responsável por este bairro. Utilizado pelo roteador de Work Orders para identificar a UO destino a partir do logradouro informado pelo cidadão. Um bairro só pode pertencer a uma abrangência.
{% enddocs %}

{% docs czrm_salesforce__col__cpf %}
Campo de identificação única (CPF) do cidadão. Configurado como ID Externo para integração com sistemas externos.=. Possui Regra de Validação para garantir exatamente 11 dígitos numéricos.
{% enddocs %}

{% docs czrm_salesforce__col__cre %}
Lookup para o registro de recorte administrativo do tipo Coordenadoria Regional de Educação
{% enddocs %}

{% docs czrm_salesforce__col__created_by_id %}
ID do criador
{% enddocs %}

{% docs czrm_salesforce__col__created_date %}
Data de criação
{% enddocs %}

{% docs czrm_salesforce__col__custom_lwc_name %}
Nome do LWC customizado a renderizar quando Field_Type__c = Customizado.
{% enddocs %}

{% docs czrm_salesforce__col__data_criacao %}
Campo `Created_date__c` do Salesforce. Na prática coincide com `created_date` em todas as linhas observadas — a distinção documentada na origem ("data informada pelo órgão") não se confirma nos dados.
{% enddocs %}

{% docs czrm_salesforce__col__data_fechamento %}
Data/hora em que a WorkOrder foi fechada (status Fechado ou Cancelado). Gravado automaticamente no after insert/update. É a fonte da janela pós-fechamento de Ouvidoria (CZRM-2369). Não confundir com WorkOrder.EndDate, que não é populado nesta org.
{% enddocs %}

{% docs czrm_salesforce__col__data_hora_andamento %}
Referência de negócio do andamento (dataHora do órgão), com checagem de sanidade. Não usar Publication_Datetime__c.
{% enddocs %}

{% docs czrm_salesforce__col__data_hora_de_resolucao %}
Campo data/hora preenchido por flow sempre que um caso é atualizado para resolvido
{% enddocs %}

{% docs czrm_salesforce__col__death_date %}
Data de falecimento
{% enddocs %}

{% docs czrm_salesforce__col__default_group_notification_frequency %}
Frequência de notificação padrão ao entrar em grupos
{% enddocs %}

{% docs czrm_salesforce__col__delegated_approver_id %}
ID do aprovador delegado
{% enddocs %}

{% docs czrm_salesforce__col__department %}
Departamento
{% enddocs %}

{% docs czrm_salesforce__col__department_group %}
Grupo de departamento
{% enddocs %}

{% docs czrm_salesforce__col__descricao_resumida %}
Campo de área de texto que recebe a descrição resumida do caso a partir de um flow
{% enddocs %}

{% docs czrm_salesforce__col__description %}
Descrição da conta
{% enddocs %}

{% docs czrm_salesforce__col__detalhe %}
Detalhe
{% enddocs %}

{% docs czrm_salesforce__col__developer_name %}
Nome do desenvolvedor
{% enddocs %}

{% docs czrm_salesforce__col__digest_frequency %}
Frequência de destaques do Chatter por e-mail
{% enddocs %}

{% docs czrm_salesforce__col__disability_type %}
Indica se o cidadão possui alguma deficiência e, em caso positivo, qual o tipo.
{% enddocs %}

{% docs czrm_salesforce__col__discount %}
Desconto
{% enddocs %}

{% docs czrm_salesforce__col__display_name %}
Nome de exibição (apelido/nickname) autodeclarado pelo cidadão no portal. Difere do nome social (identidade de gênero).
{% enddocs %}

{% docs czrm_salesforce__col__division %}
Divisão
{% enddocs %}

{% docs czrm_salesforce__col__does_include_bosses %}
Incluir superiores
{% enddocs %}

{% docs czrm_salesforce__col__does_send_email_to_members %}
Enviar e-mail aos membros
{% enddocs %}

{% docs czrm_salesforce__col__door_number %}
Numero da porta separado do logradouro. Compatibilidade com SGRC.
{% enddocs %}

{% docs czrm_salesforce__col__duration %}
Duração
{% enddocs %}

{% docs czrm_salesforce__col__duration_in_minutes %}
Duração em minutos
{% enddocs %}

{% docs czrm_salesforce__col__duration_type %}
Tipo de duração
{% enddocs %}

{% docs czrm_salesforce__col__dynamic_fields_json %}
Guarda o JSON dos campos dinamicos da secao \"Atualizacao da OS\" ate a aprovacao da WOU. Apos aprovar, os valores sao aplicados na WorkOrder.
{% enddocs %}

{% docs czrm_salesforce__col__dynamic_form_config %}
Snapshot JSON da configuração do formulário dinâmico no momento da criação da Work Order. Gravado pelo Screen Flow de criação de protocolo. Lido pelo LWC serviceAttributesViewer na Record Page.
{% enddocs %}

{% docs czrm_salesforce__col__education_level %}
Nível de escolaridade do cidadão.
{% enddocs %}

{% docs czrm_salesforce__col__em_fila_fallback %}
CZRM-1785. Verdadeiro enquanto a OS estiver na fila de falhas de roteamento. Diferente de Routing_Fallback__c, que e marca historica e permanece verdadeira mesmo depois de a OS ser reatribuida. Este campo existe para o aviso em tela aparecer apenas enquanto a OS estiver de fato parada na fila.
{% enddocs %}

{% docs czrm_salesforce__col__email %}
E-mail
{% enddocs %}

{% docs czrm_salesforce__col__email_bounced_date %}
Data de rejeição do e-mail
{% enddocs %}

{% docs czrm_salesforce__col__email_bounced_reason %}
Motivo de rejeição do e-mail
{% enddocs %}

{% docs czrm_salesforce__col__email_corporativo %}
Email corporativo do servidor (obtido do User.Email interno). Usado para auditoria de quem presta serviço público e seu email funcional.
{% enddocs %}

{% docs czrm_salesforce__col__email_encoding_key %}
Codificação de e-mail
{% enddocs %}

{% docs czrm_salesforce__col__email_institucional %}
E-mail genérico da UO (ex: ouvidoria@smtr) para alertas ou cópias automatizadas do CRM
{% enddocs %}

{% docs czrm_salesforce__col__email_notification_sent %}
Indica se o fluxo de notificação por e-mail ao cidadão já foi processado para este andamento, evitando reenvios em caso de atualizações subsequentes do registro.
{% enddocs %}

{% docs czrm_salesforce__col__email_preferences_auto_bcc %}
BCC automático
{% enddocs %}

{% docs czrm_salesforce__col__email_preferences_auto_bcc_stay_in_touch %}
BCC automático manter contato
{% enddocs %}

{% docs czrm_salesforce__col__email_preferences_stay_in_touch_reminder %}
Lembrete de manter contato
{% enddocs %}

{% docs czrm_salesforce__col__email_servidor_sent %}
Indica se a notificação por e-mail aos destinatários (servidores) já foi processada para este andamento, evitando reenvios em caso de atualizações subsequentes do registro.
{% enddocs %}

{% docs czrm_salesforce__col__employee_number %}
Número do funcionário
{% enddocs %}

{% docs czrm_salesforce__col__end_date %}
Data de término
{% enddocs %}

{% docs czrm_salesforce__col__end_day %}
Fim do dia
{% enddocs %}

{% docs czrm_salesforce__col__entitlement_id %}
ID do direito
{% enddocs %}

{% docs czrm_salesforce__col__equivalence_analysis_at %}
Momento em que a analise de equivalencia pos-criacao terminou (com ou sem agrupamento). O gate do m1 usa este carimbo para nao liberar a ocorrencia antes da relationship existir - evita chamado duplicado no orgao.
{% enddocs %}

{% docs czrm_salesforce__col__equivalent_count %}
Quantidade de WOs equivalentes vinculadas a esta WO agrupadora (vinculos com Tipo = Equivalencia).
{% enddocs %}

{% docs czrm_salesforce__col__esqueleto %}
Case de voz que chegou vazio da telefonia (sem cidadão e sem categoria) e ainda não foi completado pelo wizard. Marcado pelo CaseTrigger (CaseSkeletonService) no insert ou enviado já marcado pela integração; limpo pelo Completar.
{% enddocs %}

{% docs czrm_salesforce__col__esta_na_fila %}
Campo que indica se o caso está ou não em uma fila
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_default_et_page %}
Página padrão do Marketing Cloud
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_default_mid %}
MID padrão
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_exact_target_for_app_exchange_admin %}
Marketing Cloud para administrador do AppExchange
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_exact_target_for_app_exchange_user %}
Marketing Cloud para usuário do AppExchange
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_exact_target_o_auth_token %}
Descontinuado
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_exact_target_username %}
Nome de usuário do Marketing Cloud
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_has_opted_out_of_mobile %}
Cancelar recebimento mobile
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_has_opted_out_of_mobile_pc %}
Cancelar recebimento mobile
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_mobile_country_code %}
Código de país do celular
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_mobile_country_code_pc %}
Código de país do celular
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_valid_exact_target_admin %}
Administrador válido do Marketing Cloud
{% enddocs %}

{% docs czrm_salesforce__col__et4ae5_valid_exact_target_user %}
Usuário válido do Marketing Cloud
{% enddocs %}

{% docs czrm_salesforce__col__event_comments %}
Campo de texto longo que permite o registro do relato associado ao andamento. Acomoda pareceres descritivos e também interações via portal do cidadão.
{% enddocs %}

{% docs czrm_salesforce__col__event_type %}
Classifica o tipo do evento de andamento a partir de valores prédefinidos no Global Value Set = WorkOrderUpdate_Event_Type.
{% enddocs %}

{% docs czrm_salesforce__col__expected_attachments %}
Quantidade de anexos que o cidadao vai enviar nesta OS (informada na abertura - ex.: attachmentsCount do portal). O gate de automacao do M1 aguarda o vinculo destes anexos antes de liberar a Ocorrencia (CZRM-1670).
{% enddocs %}

{% docs czrm_salesforce__col__extension %}
Ramal
{% enddocs %}

{% docs czrm_salesforce__col__external_integration_key %}
URL/endpoint do webservice de integração definido no SGRC. Preenchido apenas para unidades com integração externa (RIOLUZ, COMLURB, DEFESA CIVIL, GM-RIO).
{% enddocs %}

{% docs czrm_salesforce__col__external_key %}
Chave de idempotência da resposta, composta por Ordem de Serviço + Atributo (ou chave própria para atributos customizados como o COMLURB). Usada para upsert sem duplicar linhas.
{% enddocs %}

{% docs czrm_salesforce__col__external_ticket_id %}
Número do chamado no sistema do órgão (codigoOcorrencia). Referência; não parear com a categoria do serviço.
{% enddocs %}

{% docs czrm_salesforce__col__family_income %}
Faixa de renda familiar mensal do cidadão, expressa em salários mínimos.
{% enddocs %}

{% docs czrm_salesforce__col__favela_code %}
Codigo da favela conforme IPP Favelas_Urbanizacao (cod_favela_1). Preenchido automaticamente pelo mapa.
{% enddocs %}

{% docs czrm_salesforce__col__favela_name %}
Nome da favela/comunidade onde a ocorrencia esta localizada. Preenchido do IPP Favelas_Urbanizacao ou GeoJSON local.
{% enddocs %}

{% docs czrm_salesforce__col__fax %}
Fax da conta
{% enddocs %}

{% docs czrm_salesforce__col__federation_identifier %}
ID de federação SAML
{% enddocs %}

{% docs czrm_salesforce__col__field_label_name %}
Rótulo amigável do atributo, exibido no formulário dinâmico. Limite de 100 caracteres, alinhado à trava do SGRC (tb_atributo.no_amigavel).
{% enddocs %}

{% docs czrm_salesforce__col__field_type %}
Define o data type do campo (Text, Picklist, etc.) que será exibido no formulário.
{% enddocs %}

{% docs czrm_salesforce__col__first_name %}
Nome
{% enddocs %}

{% docs czrm_salesforce__col__forecast_enabled %}
Permitir previsão
{% enddocs %}

{% docs czrm_salesforce__col__form_answers_json %}
Guarda o JSON das respostas do formulario do servico de destino preenchidas na transferencia, ate a aprovacao da WOU. Ao aprovar, as respostas sao gravadas em WorkOrder_Attribute_Answer__c antes de montar o payload do orgao.
{% enddocs %}

{% docs czrm_salesforce__col__form_section %}
Define em qual agrupamento visual do formulário dinâmico este atributo será exibido. Esta informação é consumida pelo componente LWC para organizar os campos em seções (Accordions) na tela de atendimento, separando dados do cidadão, informações gerais da ocorrência e campos técnicos específicos.
{% enddocs %}

{% docs czrm_salesforce__col__fsl_is_fill_in_candidate %}
É candidato a preenchimento
{% enddocs %}

{% docs czrm_salesforce__col__fsl_prevent_geocoding_for_chatter_actions %}
Impedir geocodificação para ações do Chatter
{% enddocs %}

{% docs czrm_salesforce__col__fsl_scheduling_priority %}
Prioridade de agendamento
{% enddocs %}

{% docs czrm_salesforce__col__full_photo_url %}
URL da foto em tamanho completo
{% enddocs %}

{% docs czrm_salesforce__col__gender_identity %}
Identidade de gênero
{% enddocs %}

{% docs czrm_salesforce__col__geocode_accuracy %}
Precisão do geocódigo
{% enddocs %}

{% docs czrm_salesforce__col__geolocation_precise %}
Indica se a geolocalizacao do endereco foi obtida com precisao ROOFTOP
{% enddocs %}

{% docs czrm_salesforce__col__gestor_responsavel %}
Funcionário público líder atual pela UO. Ideal para receber notificações de SLAs vencidos
{% enddocs %}

{% docs czrm_salesforce__col__global_help_text %}
Instrução de preenchimento padrão para este atributo em todos os serviços que o utilizam.
{% enddocs %}

{% docs czrm_salesforce__col__google_neighborhood_name %}
Nome do bairro conforme retornado pelo Google Places API. Usado para resolver o lookup WorkOrder.Neighborhood__c quando o nome do Google difere do nome oficial IPP.
{% enddocs %}

{% docs czrm_salesforce__col__grand_total %}
Total geral
{% enddocs %}

{% docs czrm_salesforce__col__group_type %}
Tipo
{% enddocs %}

{% docs czrm_salesforce__col__has_approval_process %}
Campo fórmula que indica se houve um processo de aprovação para o registgro de andamento.
{% enddocs %}

{% docs czrm_salesforce__col__has_equivalents %}
Indica que esta OS e uma agrupadora, ou seja, possui OS equivalentes vinculadas a ela (WorkOrder_Relationship__c com Tipo = Equivalencia). Derivado de Equivalent_Count__c, por isso e sempre consistente com o vinculo real.
{% enddocs %}

{% docs czrm_salesforce__col__has_opted_out_geo_tracking %}
Bloquear rastreamento de geolocalização
{% enddocs %}

{% docs czrm_salesforce__col__has_opted_out_processing %}
Não processar
{% enddocs %}

{% docs czrm_salesforce__col__has_opted_out_profiling %}
Não perfilar
{% enddocs %}

{% docs czrm_salesforce__col__has_opted_out_solicit %}
Não enviar marketing
{% enddocs %}

{% docs czrm_salesforce__col__has_opted_out_tracking %}
Não rastrear
{% enddocs %}

{% docs czrm_salesforce__col__has_privacy_hold %}
Retenção de privacidade
{% enddocs %}

{% docs czrm_salesforce__col__home_phone %}
Telefone residencial
{% enddocs %}

{% docs czrm_salesforce__col__id %}
ID da conta
{% enddocs %}

{% docs czrm_salesforce__col__ignore_validation_rule %}
Campo técnico que realiza bypass das regras de validação
{% enddocs %}

{% docs czrm_salesforce__col__individual_id %}
ID do indivíduo
{% enddocs %}

{% docs czrm_salesforce__col__individuals_age %}
Idade do indivíduo
{% enddocs %}

{% docs czrm_salesforce__col__industry %}
Setor
{% enddocs %}

{% docs czrm_salesforce__col__influencer_rating %}
Classificação de influenciador
{% enddocs %}

{% docs czrm_salesforce__col__integracao_ativa %}
Indica que o registro é usado na integração (IDA) e tem a edição travada no Painel SUBTD.
{% enddocs %}

{% docs czrm_salesforce__col__integration_direction %}
Marcador anti-loop: inbound = chamado originado de um evento da VOLTA (não redispara IDA).
{% enddocs %}

{% docs czrm_salesforce__col__integration_event %}
Evento de integração que originou este andamento. Discriminador do resolvedor de status (integração ⇒ CMT2) e rastro/idempotência.
{% enddocs %}

{% docs czrm_salesforce__col__integration_number %}
Número do chamado usado na integração = base + WorkOrderNumber. Fonte da faixa de roteamento (>= base = Salesforce). Formula, sem armazenamento.
{% enddocs %}

{% docs czrm_salesforce__col__integration_source %}
Origem do chamado quando criado pela integração (marcador anti-loop). Ex.: DEFESA.
{% enddocs %}

{% docs czrm_salesforce__col__internal_only %}
Somente abertura interna: quando marcado, o serviço não fica disponível para abertura de chamado pelo cidadão. Substitui Available_For_Citizen__c (semântica invertida).
{% enddocs %}

{% docs czrm_salesforce__col__ipp_street_name %}
Nome oficial do logradouro conforme cadastro IPP (Logradouro Georreferenciamento). Pode diferir de Address.Street se o cidadao digitou um nome popular.
{% enddocs %}

{% docs czrm_salesforce__col__is_abrangencia %}
Indica que o estagio de abrangencia foi executado com sucesso para esta WO.
{% enddocs %}

{% docs czrm_salesforce__col__is_active %}
Ativo
{% enddocs %}

{% docs czrm_salesforce__col__is_anonymous %}
Indica que o protocolo foi aberto anonimamente.
{% enddocs %}

{% docs czrm_salesforce__col__is_closed %}
Fechado
{% enddocs %}

{% docs czrm_salesforce__col__is_confidential %}
Serviços sigilosos não aparecem na consulta pública do portal e também na busca padrão dentro do sistema. A visibilidade da ordem de serviço fica restrita à fila da UO executora e ao perfil de Ouvidor.
{% enddocs %}

{% docs czrm_salesforce__col__is_customer_portal %}
Conta do portal do cliente.
{% enddocs %}

{% docs czrm_salesforce__col__is_deleted %}
Excluído
{% enddocs %}

{% docs czrm_salesforce__col__is_email_bounced %}
E-mail rejeitado
{% enddocs %}

{% docs czrm_salesforce__col__is_equivalencia %}
Indica que esta OS foi agrupada por uma OS agrupadora (e membro de um agrupamento de equivalencia). NAO indica que a OS e agrupadora: para isso usar Has_Equivalents__c. A agrupadora permanece com este campo false.
{% enddocs %}

{% docs czrm_salesforce__col__is_escalated %}
Escalado
{% enddocs %}

{% docs czrm_salesforce__col__is_ext_indicator_visible %}
Exibir indicador de usuário externo.
{% enddocs %}

{% docs czrm_salesforce__col__is_external_public_servant %}
Indica se a conta pertence a um servidor da PCRJ com acesso externo (via portal dos servidores)
{% enddocs %}

{% docs czrm_salesforce__col__is_favela %}
Indica se o local da ocorrencia esta dentro de uma area de favela/comunidade. Preenchido automaticamente pelo mapa via GeoJSON/IPP.
{% enddocs %}

{% docs czrm_salesforce__col__is_home_owner %}
É proprietário de imóvel
{% enddocs %}

{% docs czrm_salesforce__col__is_internal %}
Verdadeiro quando a resposta pertence à seção interna (Atualização da OS), preenchida pelo despachador e não visível ao cidadão.
{% enddocs %}

{% docs czrm_salesforce__col__is_lai %}
Sinaliza que este serviço faz parte do fluxo de Acesso à Informação (LAI). Usado pela API de consulta de chamados para classificar a ordem de serviço como \"Acesso à Informação\" na UI do Portal, sem alterar a categoria interna do serviço.
{% enddocs %}

{% docs czrm_salesforce__col__is_partner %}
Conta parceira
{% enddocs %}

{% docs czrm_salesforce__col__is_person_account %}
É conta pessoal
{% enddocs %}

{% docs czrm_salesforce__col__is_person_type %}
É tipo pessoal
{% enddocs %}

{% docs czrm_salesforce__col__is_portal_enabled %}
Indica se o portal está habilitado para o usuário.
{% enddocs %}

{% docs czrm_salesforce__col__is_priority_record %}
Indica se o contato é marcado como prioritário.
{% enddocs %}

{% docs czrm_salesforce__col__is_profile_photo_active %}
Possui foto de perfil
{% enddocs %}

{% docs czrm_salesforce__col__is_public_servant %}
Indica se a Person Account presta serviço público (servidor/terceirizado/consultor). Utilizado para controlar acesso ao portal /servidor-pcrj.
{% enddocs %}

{% docs czrm_salesforce__col__is_reiteracao %}
Reiteração
{% enddocs %}

{% docs czrm_salesforce__col__is_replicated %}
Indica que este WOU foi criado por replicacao de equivalencia. Usado como guard de recursao no WorkOrderUpdateTrigger.
{% enddocs %}

{% docs czrm_salesforce__col__is_stopped %}
Parado
{% enddocs %}

{% docs czrm_salesforce__col__is_tourist %}
Indica se o cidadao e um turista estrangeiro. Quando marcado, o campo de CPF e substituido pelo campo de Passaporte.
{% enddocs %}

{% docs czrm_salesforce__col__is_vinculacao %}
Indica que o estagio de vinculacao foi executado para esta WO.
{% enddocs %}

{% docs czrm_salesforce__col__is_visible_to_citizen %}
Campo fórmula que verifica se a configuração do evento permite visibilidade para o cidadão e se o evento de andamento já encontra-se publicado.
{% enddocs %}

{% docs czrm_salesforce__col__is_wo_closed %}
Indica se a ordem de serviço está fechada considerando os status Fechado e Cancelado (o IsClosed nativo não trata Cancelado).
{% enddocs %}

{% docs czrm_salesforce__col__jigsaw %}
Data.com Key
{% enddocs %}

{% docs czrm_salesforce__col__jigsaw_company_id %}
Jigsaw Company ID
{% enddocs %}

{% docs czrm_salesforce__col__jigsaw_contact_id %}
ID do contato Jigsaw
{% enddocs %}

{% docs czrm_salesforce__col__justification %}
Justificativa selecionada para o andamento. Vinculada ao catálogo global de justificativas. Preenchida quando o evento configurado possui justificativas associadas via Event_Config_Justification__c.
{% enddocs %}

{% docs czrm_salesforce__col__landmark %}
Ponto de referencia do local da ocorrencia para facilitar a localizacao (ex: proximo ao mercado, em frente a praca).
{% enddocs %}

{% docs czrm_salesforce__col__language %}
Idioma
{% enddocs %}

{% docs czrm_salesforce__col__language_locale_key %}
Idioma
{% enddocs %}

{% docs czrm_salesforce__col__last_access_servant_portal %}
Armazena a data e hora do último acesso do servidor ao portal de andamentos externos. Utilizado para calcular inatividade de 12 meses.
{% enddocs %}

{% docs czrm_salesforce__col__last_activity_date %}
Última atividade
{% enddocs %}

{% docs czrm_salesforce__col__last_cu_request_date %}
Data do último pedido de manter contato
{% enddocs %}

{% docs czrm_salesforce__col__last_cu_update_date %}
Data de salvamento do último manter contato
{% enddocs %}

{% docs czrm_salesforce__col__last_login_date %}
Último login
{% enddocs %}

{% docs czrm_salesforce__col__last_modified_by_id %}
ID do último modificador
{% enddocs %}

{% docs czrm_salesforce__col__last_modified_date %}
Data da última modificação
{% enddocs %}

{% docs czrm_salesforce__col__last_name %}
Sobrenome
{% enddocs %}

{% docs czrm_salesforce__col__last_referenced_date %}
Data da última referência
{% enddocs %}

{% docs czrm_salesforce__col__last_update_channel %}
Canal da última atualização
{% enddocs %}

{% docs czrm_salesforce__col__last_update_date %}
Cópia de WorkOrderUpdate__c.CreatedDate do andamento mais recente da OS pai (CreatedDate DESC, Id DESC, sem Rascunho); vazio quando não há andamento. Mantido por WorkOrderLastUpdateStatusService junto com Last_Update_Status__c (CZRM-2422) para a base Último andamento do filtro Período do Painel de Monitoramento. Não editar à mão.
{% enddocs %}

{% docs czrm_salesforce__col__last_update_status %}
Cópia de WorkOrderUpdate__c.Status__c do andamento mais recente da OS pai (CreatedDate DESC, Id DESC, sem Rascunho); vazio quando não há andamento. Mantido por WorkOrderLastUpdateStatusService (painel-filtro-status-andamento) para o filtro Status do andamento do Painel de Monitoramento. Não editar à mão.
{% enddocs %}

{% docs czrm_salesforce__col__last_viewed_date %}
Data da última visualização
{% enddocs %}

{% docs czrm_salesforce__col__latitude %}
Latitude
{% enddocs %}

{% docs czrm_salesforce__col__latitude_display %}
Exibe a latitude do endereco da OS em formato texto.
{% enddocs %}

{% docs czrm_salesforce__col__line_item_count %}
Itens de linha
{% enddocs %}

{% docs czrm_salesforce__col__link_da_corrida %}
Campo URL que grava o link da corrida no aplicativo Taxi.Rio.
{% enddocs %}

{% docs czrm_salesforce__col__locale_sid_key %}
Localidade
{% enddocs %}

{% docs czrm_salesforce__col__location_id %}
ID da localização
{% enddocs %}

{% docs czrm_salesforce__col__longitude %}
Longitude
{% enddocs %}

{% docs czrm_salesforce__col__longitude_display %}
Exibe a longitude do endereco da OS em formato texto.
{% enddocs %}

{% docs czrm_salesforce__col__mailing_address %}
Endereço de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_city %}
Cidade de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_country %}
País de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_geocode_accuracy %}
Precisão do geocódigo de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_latitude %}
Latitude de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_longitude %}
Longitude de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_postal_code %}
CEP de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_state %}
Estado/Província de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__mailing_street %}
Logradouro de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__main_organizational_unit %}
Órgão central do servidor (sincronizado do User.main_organizational_unit__c). Picklist governada pelo GVS main_organizational_unit.
{% enddocs %}

{% docs czrm_salesforce__col__manager_id %}
ID do gerente
{% enddocs %}

{% docs czrm_salesforce__col__master_record_id %}
ID do registro mestre
{% enddocs %}

{% docs czrm_salesforce__col__medium_banner_photo_url %}
URL da foto de banner para Android
{% enddocs %}

{% docs czrm_salesforce__col__medium_photo_url %}
URL da foto de perfil média
{% enddocs %}

{% docs czrm_salesforce__col__milestone_status %}
Status do marco
{% enddocs %}

{% docs czrm_salesforce__col__milestone_timer %}
Tempo para o próximo marco
{% enddocs %}

{% docs czrm_salesforce__col__military_service %}
Serviço militar
{% enddocs %}

{% docs czrm_salesforce__col__mobile_phone %}
Telefone celular
{% enddocs %}

{% docs czrm_salesforce__col__modification_reason %}
Justificativa fornecida pelo Despachador Central quando o andamento foi aprovado com modificações no relato original. Preenchido pelo screen flow de aprovação.
{% enddocs %}

{% docs czrm_salesforce__col__modified_comments %}
Relato atual do andamento. Preenchido automaticamente na criação com o relato original e atualizado quando o Despachador Central aprova com modificação.
{% enddocs %}

{% docs czrm_salesforce__col__motivo_nao_execucao %}
Motivo pelo qual o serviço não pôde ser executado. Preenchido pelo executor no app mobile. Fonte: Carta de Serviço 1746 (PPTX 10/06/2025).
{% enddocs %}

{% docs czrm_salesforce__col__name %}
Nome da conta
{% enddocs %}

{% docs czrm_salesforce__col__namespace_prefix %}
Prefixo de namespace
{% enddocs %}

{% docs czrm_salesforce__col__nationality %}
Nacionalidade do cidadão.
{% enddocs %}

{% docs czrm_salesforce__col__natureza_orgao %}
Exclusivo do Record Type 'Órgão'. Classifica a natureza institucional da UO.
{% enddocs %}

{% docs czrm_salesforce__col__neighborhood %}
Bairro
{% enddocs %}

{% docs czrm_salesforce__col__no_number %}
Indica que o local da ocorrencia nao possui numero. Quando marcado, o campo de nome de via nao oficial pode ser preenchido.
{% enddocs %}

{% docs czrm_salesforce__col__nome_cidadao_assinatura %}
Campo que define o nome do usuário que abriu o ticket, consolidando dos campos existentes no objeto contato e no campo SuppliedName.
{% enddocs %}

{% docs czrm_salesforce__col__nome_completo %}
Campo fórmula para exibir o nome completo sem exibir o campo de tratamento.
{% enddocs %}

{% docs czrm_salesforce__col__nome_do_proprietario %}
Fórmula de texto que indica o nome do proprietário do caso
{% enddocs %}

{% docs czrm_salesforce__col__nome_equipe_assinatura %}
Campo para compor o template padrão de resposta por e-mail
{% enddocs %}

{% docs czrm_salesforce__col__nome_ficticio %}
Campo de nome fictício para ser usado por alguns times de atendimento que tem como procedimento não expor o nome real do atendente (Ex: Fila do atendimento Táxi Rio).
{% enddocs %}

{% docs czrm_salesforce__col__notificacao_proprietario_sent %}
Indica se a notificação ao proprietário da OS sobre a resposta a um andamento externo já foi processada, evitando reenvios em retriggers do registro.
{% enddocs %}

{% docs czrm_salesforce__col__number_of_employees %}
Funcionários
{% enddocs %}

{% docs czrm_salesforce__col__occupation %}
Ocupação
{% enddocs %}

{% docs czrm_salesforce__col__offline_pda_trial_expiration_date %}
Data de expiração do período de teste Sales Anywhere
{% enddocs %}

{% docs czrm_salesforce__col__offline_trial_expiration_date %}
Data de expiração do período de teste offline
{% enddocs %}

{% docs czrm_salesforce__col__ombudsman_exclusive %}
Marca registros exclusivos de Ouvidoria (Tema/Subtema/Serviço). Não aparecem na carta de serviços pública.
{% enddocs %}

{% docs czrm_salesforce__col__operador_matricula %}
Matrícula do operador informada pelo órgão no reverso (texto; não é usuário do Salesforce).
{% enddocs %}

{% docs czrm_salesforce__col__operador_nome %}
Nome do operador informado pelo orgao no reverso departamental (m14).
{% enddocs %}

{% docs czrm_salesforce__col__operador_origem %}
Origem do operador informado pelo órgão no reverso (ex.: PCRJ). Texto livre; não é usuário do Salesforce.
{% enddocs %}

{% docs czrm_salesforce__col__operador_telefone %}
Telefone do operador informado pelo orgao no reverso departamental (m14).
{% enddocs %}

{% docs czrm_salesforce__col__origin %}
Origem do protocolo
{% enddocs %}

{% docs czrm_salesforce__col__origin_service %}
Serviço de origem de uma transferência de OS (Event_Type__c = Transfer_WO). Gravado no registro do andamento com o WorkOrder.Service__c vigente no momento da solicitação, porque a aprovação da transferência sobrescreve o serviço da OS com o destino e a origem deixaria de ser recuperável. Permite exibir \"Transferida de [origem] para [destino]\" na linha do tempo.
{% enddocs %}

{% docs czrm_salesforce__col__os_owner %}
Campo fórmula que indica o proprietário da Ordem de Serviço, com atalho para visualização do usuário ou dos membros da fila da UO
{% enddocs %}

{% docs czrm_salesforce__col__other_phone %}
Outro telefone
{% enddocs %}

{% docs czrm_salesforce__col__out_of_office_message %}
Mensagem de ausência
{% enddocs %}

{% docs czrm_salesforce__col__oversight_target_uo %}
UO alvo cujos chamados (e camadas abaixo) são fiscalizados por esta Unidade Organizacional. Preenchido SOMENTE em UOs com Unity_type__c = Ouvidoria. O ouvidor enxerga os chamados da UO Fiscalizada mais toda a subárvore descendente (via Parent_UO__c), somente leitura. Uma ouvidoria fiscaliza uma única UO.
{% enddocs %}

{% docs czrm_salesforce__col__owner_id %}
ID do proprietário
{% enddocs %}

{% docs czrm_salesforce__col__ownership %}
Propriedade
{% enddocs %}

{% docs czrm_salesforce__col__parent_id %}
ID da conta pai
{% enddocs %}

{% docs czrm_salesforce__col__parent_uo %}
Aponta para a unidade imediatamente superior (N-1). Obrigatório para Unidade Administrativa e Unidade de Ouvidoria.
{% enddocs %}

{% docs czrm_salesforce__col__parent_work_order_id %}
ID da ordem de serviço pai
{% enddocs %}

{% docs czrm_salesforce__col__passport_number %}
Numero do passaporte do cidadao estrangeiro. Obrigatorio quando IsTourist__c = true. Formato: 5 a 20 caracteres alfanumericos.
{% enddocs %}

{% docs czrm_salesforce__col__password_expiration_date %}
Data de expiração da senha
{% enddocs %}

{% docs czrm_salesforce__col__permite_atribuir_chamados %}
Indica se a UO está habilitada para receber demandas através de filas ou se é uma UO de caráter somente gerencial
{% enddocs %}

{% docs czrm_salesforce__col__person_assistant_name %}
Assistente
{% enddocs %}

{% docs czrm_salesforce__col__person_assistant_phone %}
Telefone do assistente
{% enddocs %}

{% docs czrm_salesforce__col__person_birthdate %}
Data de nascimento
{% enddocs %}

{% docs czrm_salesforce__col__person_contact_id %}
ID do contato
{% enddocs %}

{% docs czrm_salesforce__col__person_department %}
Departamento
{% enddocs %}

{% docs czrm_salesforce__col__person_do_not_call %}
Não ligar
{% enddocs %}

{% docs czrm_salesforce__col__person_email %}
E-mail
{% enddocs %}

{% docs czrm_salesforce__col__person_email_bounced_date %}
Data de rejeição do e-mail
{% enddocs %}

{% docs czrm_salesforce__col__person_email_bounced_reason %}
Motivo de rejeição do e-mail
{% enddocs %}

{% docs czrm_salesforce__col__person_gender_identity %}
Identidade de gênero
{% enddocs %}

{% docs czrm_salesforce__col__person_has_opted_out_of_email %}
Cancelar recebimento de e-mail
{% enddocs %}

{% docs czrm_salesforce__col__person_has_opted_out_of_fax %}
Cancelar recebimento de fax
{% enddocs %}

{% docs czrm_salesforce__col__person_home_phone %}
Telefone residencial
{% enddocs %}

{% docs czrm_salesforce__col__person_individual_id %}
ID do indivíduo
{% enddocs %}

{% docs czrm_salesforce__col__person_last_cu_request_date %}
Data do último pedido de manter contato
{% enddocs %}

{% docs czrm_salesforce__col__person_last_cu_update_date %}
Data de salvamento do último manter contato
{% enddocs %}

{% docs czrm_salesforce__col__person_lead_source %}
Origem do lead
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_address %}
Endereço de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_city %}
Cidade de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_country %}
País de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_geocode_accuracy %}
Precisão do geocódigo de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_latitude %}
Latitude de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_longitude %}
Longitude de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_postal_code %}
CEP de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_state %}
Estado/Província de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mailing_street %}
Logradouro de correspondência
{% enddocs %}

{% docs czrm_salesforce__col__person_mobile_phone %}
Telefone celular do cidadão no formato 55 + DDD + número.
{% enddocs %}

{% docs czrm_salesforce__col__person_other_address %}
Outro endereço
{% enddocs %}

{% docs czrm_salesforce__col__person_other_city %}
Outra cidade
{% enddocs %}

{% docs czrm_salesforce__col__person_other_country %}
Outro país
{% enddocs %}

{% docs czrm_salesforce__col__person_other_geocode_accuracy %}
Precisão do outro geocódigo
{% enddocs %}

{% docs czrm_salesforce__col__person_other_latitude %}
Outra latitude
{% enddocs %}

{% docs czrm_salesforce__col__person_other_longitude %}
Outra longitude
{% enddocs %}

{% docs czrm_salesforce__col__person_other_phone %}
Outro telefone
{% enddocs %}

{% docs czrm_salesforce__col__person_other_postal_code %}
Outro CEP
{% enddocs %}

{% docs czrm_salesforce__col__person_other_state %}
Outro estado/província
{% enddocs %}

{% docs czrm_salesforce__col__person_other_street %}
Outro logradouro
{% enddocs %}

{% docs czrm_salesforce__col__person_pronouns %}
Pronomes
{% enddocs %}

{% docs czrm_salesforce__col__person_title %}
Título
{% enddocs %}

{% docs czrm_salesforce__col__phone %}
Telefone da conta.
{% enddocs %}

{% docs czrm_salesforce__col__phone_1 %}
Telefone principal do cidadao. Substitui o campo PersonMobilePhone. Formato hibrido: celular (55 + DDD + 9 + 8 digitos = 13) ou fixo (55 + DDD + 8 digitos = 12). Utilizado como primeiro canal de contato e para higienizacao de cadastro.
{% enddocs %}

{% docs czrm_salesforce__col__phone_1_type %}
Identifica automaticamente o tipo do Telefone principal com base no numero de digitos: 13 digitos = Celular (55+DDD+9+8), 12 digitos = Fixo (55+DDD+8).
{% enddocs %}

{% docs czrm_salesforce__col__phone_2 %}
Telefone secundario do cidadao. Formato hibrido: celular (55+DDD+9+8 digitos) ou fixo (55+DDD+8 digitos). Utilizado no processo de higienizacao: se Phone_1__c falhar, Phone_2__c e promovido para Phone_1__c.
{% enddocs %}

{% docs czrm_salesforce__col__phone_2_type %}
Identifica automaticamente o tipo do Telefone secundario com base no numero de digitos: 13 digitos = Celular (55+DDD+9+8), 12 digitos = Fixo (55+DDD+8).
{% enddocs %}

{% docs czrm_salesforce__col__phone_3 %}
Telefone terciario do cidadao. Formato hibrido: celular (55+DDD+9+8 digitos) ou fixo (55+DDD+8 digitos). Utilizado no processo de higienizacao como terceira opcao apos Phone_1__c e Phone_2__c.
{% enddocs %}

{% docs czrm_salesforce__col__phone_3_type %}
Identifica automaticamente o tipo do Telefone terciario com base no numero de digitos: 13 digitos = Celular (55+DDD+9+8), 12 digitos = Fixo (55+DDD+8).
{% enddocs %}

{% docs czrm_salesforce__col__phone_international %}
Telefone internacional do cidadao, destinado principalmente a turistas. Aceita qualquer formato de numero internacional sem validacao de DDI especifico.
{% enddocs %}

{% docs czrm_salesforce__col__photo_url %}
URL da foto
{% enddocs %}

{% docs czrm_salesforce__col__portal_role %}
Nível de papel no portal.
{% enddocs %}

{% docs czrm_salesforce__col__possui_data_hora_resolucao_formula %}
campo formula para controlar o campo data/hora resolução no layout do caso
{% enddocs %}

{% docs czrm_salesforce__col__possui_pesquisa_respondida %}
Possui pesquisa respondida
{% enddocs %}

{% docs czrm_salesforce__col__post_work_summary %}
Enter a summary manually. Or if you’re using Agentforce, your AI agent can fill in the summary for you.
{% enddocs %}

{% docs czrm_salesforce__col__postal_code %}
CEP
{% enddocs %}

{% docs czrm_salesforce__col__pre_work_brief_prompt_template %}
Insira o ID do seu modelo de prompt "Pre-Work Brief" (Resumo de Pré-trabalho) ativado. Para encontrar o ID, abra o modelo no Prompt Builder e copie os caracteres que aparecem após "EinsteinPromptStudio/" na URL.
{% enddocs %}

{% docs czrm_salesforce__col__pricebook2_id %}
ID do catálogo de preços
{% enddocs %}

{% docs czrm_salesforce__col__priority %}
Prioridade
{% enddocs %}

{% docs czrm_salesforce__col__protocolo_formula %}
Gera o número de protocolo oficial no formato RIO-YY-XXXXXXXX
{% enddocs %}

{% docs czrm_salesforce__col__protocolo_hyperlink %}
Gera o número de protocolo oficial no formato RIO-YYYYXXXXXXXX (16 caracteres). Utiliza o ano de criação do registro e uma lógica de dispersão baseada na Semente do Protocolo para evitar números sequenciais, dificultando o scraping e garantindo 100 milhões de combinações anuais.
{% enddocs %}

{% docs czrm_salesforce__col__protocolo_texto %}
Gera o número de protocolo oficial no formato RIO-YYYYXXXXXXXX (16 caracteres). Utiliza o ano de criação do registro e uma lógica de dispersão baseada na Semente do Protocolo para evitar números sequenciais, dificultando o scraping e garantindo 100 milhões de combinações anuais.
{% enddocs %}

{% docs czrm_salesforce__col__public_servant_status %}
Indica o status de acesso do servidor ao portal de andamentos externos. Controlado automaticamente por login e scheduled job de inatividade.
{% enddocs %}

{% docs czrm_salesforce__col__public_way %}
Nome do logradouro extraido do Google Places (sem numero). Ex: Avenida Lucio Costa
{% enddocs %}

{% docs czrm_salesforce__col__public_way_code %}
Codigo do Logradouro (CL) IPP da via da ocorrencia. Preenchido via enriquecimento Apex apos confirmacao do endereco.
{% enddocs %}

{% docs czrm_salesforce__col__publication_datetime %}
Data e hora em que o andamento foi publicado. Preenchido via flow quando o status passa para Publicado, com ou sem aprovação. Campo único de publicação (CZRM-2306).
{% enddocs %}

{% docs czrm_salesforce__col__qtd_emails_enviados %}
Campo que mede a quantidade de emails enviados dentro de um protocolo
{% enddocs %}

{% docs czrm_salesforce__col__qtd_emails_recebidos %}
Campo que mede a quantidade de e-mails recebidos dentro de um protocolo
{% enddocs %}

{% docs czrm_salesforce__col__queue_id %}
Guarda o ID (00G) da fila associada a UO
{% enddocs %}

{% docs czrm_salesforce__col__queue_routing_config_id %}
ID de configuração de roteamento
{% enddocs %}

{% docs czrm_salesforce__col__ra %}
Lookup para o registro de recorte administrativo do tipo RA
{% enddocs %}

{% docs czrm_salesforce__col__race_color %}
Cor ou raça autodeclarada pelo cidadão, conforme categorias do IBGE.
{% enddocs %}

{% docs czrm_salesforce__col__rating %}
Classificação da conta
{% enddocs %}

{% docs czrm_salesforce__col__receber_email_notificacao %}
Indica se o prestador deve receber e-mail de notificação ao ser acionado em um andamento externo. Desmarcado, o prestador continua vendo e respondendo o andamento no portal, mas não recebe o e-mail. Default true.
{% enddocs %}

{% docs czrm_salesforce__col__receives_admin_info_emails %}
E-mails de informações do administrador
{% enddocs %}

{% docs czrm_salesforce__col__receives_info_emails %}
E-mails informativos
{% enddocs %}

{% docs czrm_salesforce__col__record_type_developer_name %}
Indica o nome API do tipo de registro de Unidade Organizacional
{% enddocs %}

{% docs czrm_salesforce__col__record_type_id %}
ID do tipo de registro
{% enddocs %}

{% docs czrm_salesforce__col__rejection_reason %}
Campo destinado a armazenar a justificativa técnica ou administrativa fornecida pelo Despachador Central ao recusar a publicação ou validade deste andamento. Preenchido ativamente durante o fluxo de aprovação. Exibe valores do campo GVS = WorkOrderUpdate_Rejection_Reason
{% enddocs %}

{% docs czrm_salesforce__col__related_id %}
ID relacionado
{% enddocs %}

{% docs czrm_salesforce__col__related_work_order %}
OS citada por este andamento quando ele é registrado em outra OS (ex.: aviso de equivalência gravado na agrupadora, apontando a OS agrupada). Alimenta o atalho da linha do tempo.
{% enddocs %}

{% docs czrm_salesforce__col__reports_to_id %}
ID do contato superior (hierarquia de contatos).
{% enddocs %}

{% docs czrm_salesforce__col__response_deadline %}
Armazena a data e hora limite calculada para resposta do cidadão. Campo só é aplicável quanto o tipo de andamento é \"Pedido de informação\". Caso não haja resposta dentro desse prazo, a ordem de serviço é cancelada automaticamente.
{% enddocs %}

{% docs czrm_salesforce__col__response_time_days %}
Quantidade de dias corridos definidos como prazo para aguardar a resposta do cidadão. A OS será encerrada automaticamente caso o cidadão não responda dentro do prazo. Campo só é aplicável quanto o tipo de andamento é \"Pedido de informação\".
{% enddocs %}

{% docs czrm_salesforce__col__response_to %}
Andamento de origem (pedido de informação ao servidor) que esta resposta atende. Vínculo explícito resposta→pedido, permitindo rastrear qual pedido foi respondido e rejeitar duplicatas por pedido.
{% enddocs %}

{% docs czrm_salesforce__col__responsible_org_unit %}
Órgão municipal (Nível 1) que detém a responsabilidade administrativa sobre a execução do serviço.
{% enddocs %}

{% docs czrm_salesforce__col__role_id %}
Guarda o ID do papel associado à UO
{% enddocs %}

{% docs czrm_salesforce__col__root_work_order_id %}
ID da ordem de serviço raiz
{% enddocs %}

{% docs czrm_salesforce__col__routing_fallback %}
CZRM-1785. Marcado quando o roteamento por abrangencia nao concluiu e a OS foi atribuida a fila de falhas de roteamento. Diferente de Is_Abrangencia__c, que apenas indica que o estagio de roteamento foi executado.
{% enddocs %}

{% docs czrm_salesforce__col__rp %}
Lookup para o registro de recorte administrativo do tipo RP
{% enddocs %}

{% docs czrm_salesforce__col__salutation %}
Tratamento
{% enddocs %}

{% docs czrm_salesforce__col__section %}
Seção do formulário dinâmico à qual o atributo pertencia (abertura ou atualização da OS). Usada para separar as respostas visíveis ao cidadão das internas.
{% enddocs %}

{% docs czrm_salesforce__col__send_individual_data %}
Exportar dados do indivíduo.
{% enddocs %}

{% docs czrm_salesforce__col__sender_email %}
Endereço do remetente do e-mail
{% enddocs %}

{% docs czrm_salesforce__col__sender_name %}
Nome do remetente do e-mail
{% enddocs %}

{% docs czrm_salesforce__col__service %}
Tema/assunto mestre do serviço solicitado. Referência ao catálogo de serviços da Prefeitura, identificando \"o quê\" está sendo atendido.
{% enddocs %}

{% docs czrm_salesforce__col__service_appointment_count %}
Quantidade de agendamentos de serviço
{% enddocs %}

{% docs czrm_salesforce__col__service_attribute_library %}
Atributo do formulário dinâmico respondido nesta linha. Chave usada pelo roteamento condicional e pelo SLA para localizar o valor da resposta.
{% enddocs %}

{% docs czrm_salesforce__col__service_catalog_formula %}
Nome e categoria do serviço em texto puro. Consumido por e-mails transacionais (nova solicitação, andamento da OS, pedido de informação) — NÃO adicionar HYPERLINK() aqui, pois o valor é injetado sem sanitização no corpo dos e-mails. Para exibição clicável em UI usar Service_Hyperlink__c.
{% enddocs %}

{% docs czrm_salesforce__col__service_category %}
Categoria do serviço (Solicitação, Reclamação, Informação, etc.). Define o SLA, regras de roteamento, campos de formulário e outros parâmetros aplicáveis a esta OS.
{% enddocs %}

{% docs czrm_salesforce__col__service_contract_id %}
ID do contrato de serviço
{% enddocs %}

{% docs czrm_salesforce__col__service_form_config %}
Configuração do formulário (serviço + atributo) vigente no momento da resposta: guarda ordem, obrigatoriedade, seção e dependência condicional. Lookup (não Mestre-Detalhe) para preservar o histórico mesmo que o campo saia do formulário.
{% enddocs %}

{% docs czrm_salesforce__col__service_hierarchy %}
Campo fórmula que exibe automaticamente a hierarquia Tema > Subtema > Serviço em texto único.
{% enddocs %}

{% docs czrm_salesforce__col__service_hyperlink %}
Exibe o nome e a categoria do serviço como hyperlink para a categoria do serviço. Uso exclusivo de UI (compact layout, list views). NÃO usar em e-mails ou comunicações externas: o valor contém marcação HTML de âncora.
{% enddocs %}

{% docs czrm_salesforce__col__service_hyperlink_formula %}
Campo fórmula que permite exibição de hyperlink para o registro a partir de listviews.
{% enddocs %}

{% docs czrm_salesforce__col__service_hyperlink_without_category %}
Exibe o nome do serviço como hyperlink para a categoria do serviço. Uso exclusivo de UI (compact layout, list views). NÃO usar em e-mails ou comunicações externas: o valor contém marcação HTML de âncora.
{% enddocs %}

{% docs czrm_salesforce__col__service_name %}
Campo preenchido via flow com intuito de permitr gravar o nome do serviço com mais de 80 caracteres. Terceiro nível de classificação (N3), identificando especificamente o serviço que está sendo solicitado.
{% enddocs %}

{% docs czrm_salesforce__col__sgrc_legacy_crm_subtype_id %}
Indica o código do subtipo no sistema de CZRM legado (SGRC).
{% enddocs %}

{% docs czrm_salesforce__col__sgrc_uo_id %}
ID numérico da unidade organizacional no sistema legado SGRC. Usado como External Id para integração e resolução de hierarquia.
{% enddocs %}

{% docs czrm_salesforce__col__share_citizen_data %}
Indica se os dados do cidadão (nome, endereço) devem ser exibidos ao servidor no portal de andamentos externos. Marcado pelo despachador no momento do envio do andamento.
{% enddocs %}

{% docs czrm_salesforce__col__shipping_address %}
Endereço de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_city %}
Cidade de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_country %}
País de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_geocode_accuracy %}
Precisão do geocódigo de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_latitude %}
Latitude de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_longitude %}
Longitude de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_postal_code %}
CEP de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_state %}
Estado/Província de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shipping_street %}
Logradouro de entrega
{% enddocs %}

{% docs czrm_salesforce__col__shortname %}
Nome reduzido da unidade organizacional, usado como identificador único para filas de atendimento. Máximo 40 caracteres.
{% enddocs %}

{% docs czrm_salesforce__col__should_forget %}
Esquecer este indivíduo
{% enddocs %}

{% docs czrm_salesforce__col__sic %}
Código SIC
{% enddocs %}

{% docs czrm_salesforce__col__sic_desc %}
Descrição SIC
{% enddocs %}

{% docs czrm_salesforce__col__sici_code %}
Código da Unidade Administrativa (UA) no SICI (Sistema Integrado de Codificação Institucional). Usado como identificador externo para cruzamento com bases municipais e futuras integrações. Consulta: https://sici.rio.rj.gov.br/PAG/principal.aspx
{% enddocs %}

{% docs czrm_salesforce__col__signature %}
Assinatura de e-mail
{% enddocs %}

{% docs czrm_salesforce__col__site %}
Site da conta
{% enddocs %}

{% docs czrm_salesforce__col__sla_exit_date %}
Hora de término do SLA
{% enddocs %}

{% docs czrm_salesforce__col__sla_start_date %}
Hora de início do SLA
{% enddocs %}

{% docs czrm_salesforce__col__sla_target_date %}
Prazo do serviço
{% enddocs %}

{% docs czrm_salesforce__col__slug %}
Slug URL-safe gerado automaticamente a partir do Nome. Único e imutável.
{% enddocs %}

{% docs czrm_salesforce__col__small_banner_photo_url %}
URL da foto de banner para iOS
{% enddocs %}

{% docs czrm_salesforce__col__small_photo_url %}
Foto
{% enddocs %}

{% docs czrm_salesforce__col__sobject_type %}
Nome do tipo de SObject
{% enddocs %}

{% docs czrm_salesforce__col__social_name %}
Nome social do cidadao, quando informado. Exibido quando o checkbox Possui nome social estiver marcado no formulario de atendimento.
{% enddocs %}

{% docs czrm_salesforce__col__solicitante_departamental %}
Nome do solicitante do reverso departamental (m14 da Defesa). Texto livre — o órgão/documento (OFICIO/MANDADO/SUBPDEC); sem Person Account/CPF.
{% enddocs %}

{% docs czrm_salesforce__col__solicitante_nome %}
Nome do solicitante do reverso (m14). Texto livre - sem Person Account/CPF.
{% enddocs %}

{% docs czrm_salesforce__col__solicitante_telefone %}
Telefone do solicitante informado pelo orgao no reverso departamental (m14).
{% enddocs %}

{% docs czrm_salesforce__col__source_id %}
ID da origem do protocolo
{% enddocs %}

{% docs czrm_salesforce__col__source_system_identifier %}
ID do sistema de origem
{% enddocs %}

{% docs czrm_salesforce__col__speaking_language %}
Idioma preferencial do cidadao para comunicacao. Campo opcional de perfil.
{% enddocs %}

{% docs czrm_salesforce__col__start_date %}
Data de início
{% enddocs %}

{% docs czrm_salesforce__col__start_day %}
Início do dia
{% enddocs %}

{% docs czrm_salesforce__col__state %}
Estado/Província
{% enddocs %}

{% docs czrm_salesforce__col__status %}
Status
{% enddocs %}

{% docs czrm_salesforce__col__status_category %}
Categoria do status
{% enddocs %}

{% docs czrm_salesforce__col__status_especifico_orgao %}
statusEspecifico cru informado pelo órgão (contrato da VOLTA — não perder). Base do de-para CMT1.
{% enddocs %}

{% docs czrm_salesforce__col__status_normalizado %}
Status do órgão já normalizado (de-para 1 / CMT1). Distingue casos que reusam o mesmo evento (ex.: 21 x 16).
{% enddocs %}

{% docs czrm_salesforce__col__stay_in_touch_note %}
Nota do e-mail de manter contato
{% enddocs %}

{% docs czrm_salesforce__col__stay_in_touch_signature %}
Assinatura do e-mail de manter contato
{% enddocs %}

{% docs czrm_salesforce__col__stay_in_touch_subject %}
Assunto do e-mail de manter contato
{% enddocs %}

{% docs czrm_salesforce__col__stop_start_date %}
Parado desde
{% enddocs %}

{% docs czrm_salesforce__col__street %}
Logradouro
{% enddocs %}

{% docs czrm_salesforce__col__su_access_expiration_date %}
Data de expiração do acesso SU
{% enddocs %}

{% docs czrm_salesforce__col__subcategoria %}
Subcategoria
{% enddocs %}

{% docs czrm_salesforce__col__subject %}
Assunto
{% enddocs %}

{% docs czrm_salesforce__col__subprefeitura %}
Lookup para o registro de recorte administrativo do tipo Subprefeitura.
{% enddocs %}

{% docs czrm_salesforce__col__substatus %}
Detalha o status do protocolo. Atualizado automaticamente através de flow
{% enddocs %}

{% docs czrm_salesforce__col__subtheme %}
Segundo nível de classificação (N2), que detalha a área do serviço vinculada ao Tema selecionado.
{% enddocs %}

{% docs czrm_salesforce__col__subtheme_formula %}
Campo fórmula que permite exibição do subtema sem formato lookup.
{% enddocs %}

{% docs czrm_salesforce__col__subtotal %}
Subtotal
{% enddocs %}

{% docs czrm_salesforce__col__supplied_company %}
Empresa informada pelo solicitante externo.
{% enddocs %}

{% docs czrm_salesforce__col__supplied_email %}
Endereço de e-mail
{% enddocs %}

{% docs czrm_salesforce__col__supplied_name %}
Nome
{% enddocs %}

{% docs czrm_salesforce__col__supplied_phone %}
Telefone
{% enddocs %}

{% docs czrm_salesforce__col__survey_invitation_sent %}
Indica se houve envio de convite de pesquisa para esse protocolo.
{% enddocs %}

{% docs czrm_salesforce__col__system_modstamp %}
Carimbo de modificação do sistema
{% enddocs %}

{% docs czrm_salesforce__col__target_account %}
Servidor destinatário individual de um andamento externo (\"Pedido de informação ao servidor\"). Mutuamente exclusivo com Target_Group__c.
{% enddocs %}

{% docs czrm_salesforce__col__target_group %}
Grupo de acionamento destino de um andamento externo (\"Pedido de informação ao servidor\"). Mutuamente exclusivo com Target_Account__c.
{% enddocs %}

{% docs czrm_salesforce__col__target_uo_id %}
Destino da mudança de fila/troca de UO (Change_Queue_UO). Aplicado na WorkOrder somente após a aprovação.
{% enddocs %}

{% docs czrm_salesforce__col__tax %}
Imposto
{% enddocs %}

{% docs czrm_salesforce__col__telefone_institucional %}
Contato telefônico geral não pessoal do órgão (permanente ao longo das gestões)
{% enddocs %}

{% docs czrm_salesforce__col__tem_dados_departamentais %}
True quando o protocolo do reverso possui dados departamentais (solicitante/operador nome ou telefone). Governa a visibilidade condicional no layout.
{% enddocs %}

{% docs czrm_salesforce__col__theme %}
Primeiro nível de classificação (N1), agrupando serviços por grandes áreas temáticas da prefeitura.
{% enddocs %}

{% docs czrm_salesforce__col__theme_formula %}
Campo fórmula que permite exibição do tema sem formato lookup.
{% enddocs %}

{% docs czrm_salesforce__col__ticker_symbol %}
Símbolo do ticker
{% enddocs %}

{% docs czrm_salesforce__col__time_zone_sid_key %}
Fuso horário
{% enddocs %}

{% docs czrm_salesforce__col__tipo %}
Indica a natureza do vínculo entre as ordens de serviço.
{% enddocs %}

{% docs czrm_salesforce__col__tipo_de_registro_name_formula %}
Campo formula que indica o tipo de registro do caso. Criado para impedir que usuários tentem converter o tipo de registro a partir do layout da página
{% enddocs %}

{% docs czrm_salesforce__col__tipo_de_usuario %}
Indica o perfil do usuário que está entrando em contato
{% enddocs %}

{% docs czrm_salesforce__col__tipo_solicitante %}
Tipo do solicitante do chamado. Preenchido no reverso da Defesa (m14); um protocolo pode ter chamados do cidadão e internos/servidor.
{% enddocs %}

{% docs czrm_salesforce__col__title %}
Título
{% enddocs %}

{% docs czrm_salesforce__col__title_type %}
Nível de senioridade
{% enddocs %}

{% docs czrm_salesforce__col__total_price %}
Preço total
{% enddocs %}

{% docs czrm_salesforce__col__transaction_id %}
Campo exclusivo para registrar a transação recebida de sistemas externos dos órgãos através de integração com Mulesoft
{% enddocs %}

{% docs czrm_salesforce__col__transfer_target %}
Armazena o ID do serviço destino e da categoria config para transferência de OS. Formato: {serviceId},{categoryConfigId}.
{% enddocs %}

{% docs czrm_salesforce__col__unique_key %}
Slug do rótulo do atributo, gerado por trigger (SlugService). Usado para impedir dois atributos com o mesmo rótulo dentro da org. Não é referencial externo — para integração use SGRC_Attribute_Id__c.
{% enddocs %}

{% docs czrm_salesforce__col__unity_type %}
Indica o tipo da unidade organizacional: Órgão (N1), Unidade Administrativa (subordinada hierarquica) ou Unidade de Ouvidoria (ouvidoria setorial).
{% enddocs %}

{% docs czrm_salesforce__col__uo_informada_orgao %}
Unidade organizacional informada pelo órgão no andamento (siglaUnidadeOrganizacional). Texto, sem match com a base do Salesforce.
{% enddocs %}

{% docs czrm_salesforce__col__user_external %}
Usuário externo (portal/cidadão) vinculado a esta Person Account. Espelha o Usuário Interno para dar visibilidade de qual User de comunidade representa o cadastro. Preenchido automaticamente pelo UserSyncHandler.
{% enddocs %}

{% docs czrm_salesforce__col__user_internal %}
Usuário interno (licenca Standard) vinculado a esta Person Account. Amarra o User interno a conta do servidor.
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_avantgo_user %}
Usuário AvantGo
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_call_center_auto_login %}
Login automático no call center
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_interaction_user %}
Usuário de fluxo
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_knowledge_user %}
Usuário de knowledge
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_marketing_user %}
Usuário de marketing
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_offline_user %}
Usuário offline
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_sf_content_user %}
Usuário de conteúdo Salesforce CRM
{% enddocs %}

{% docs czrm_salesforce__col__user_permissions_support_user %}
Usuário do Service Cloud
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_action_launcher_einstein_gpt_consent %}
Consentimento Einstein GPT no lançador de ações
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_activity_reminders_popup %}
Pop-up de lembretes de atividades
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_assistive_actions_enabled_in_action_launcher %}
Ações assistivas habilitadas no lançador
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_bre_lookup_table_welcome_mat %}
Tela inicial tabela de lookup BRE
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_cache_diagnostics %}
Diagnósticos de cache
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_create_lex_apps_wt_shown %}
Tutorial criação de apps LEX exibido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_dis_comment_after_like_email %}
Desabilitar e-mail de comentário após curtida.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_dis_mentions_comment_email %}
Desabilitar e-mail de menções em comentários.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_dis_prof_post_comment_email %}
Desabilitar e-mail de comentário em post de perfil.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_all_feeds_email %}
Desabilitar e-mails de todos os feeds
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_bookmark_email %}
Desabilitar e-mail de favoritos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_change_comment_email %}
Desabilitar e-mail de alteração de comentário.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_endorsement_email %}
Desabilitar e-mail de endosso
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_file_share_notifications_for_api %}
Desabilitar notificações de compartilhamento de arquivo para API
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_followers_email %}
Desabilitar e-mail de seguidores
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_later_comment_email %}
Desabilitar e-mail de comentário posterior.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_like_email %}
Desabilitar e-mail de curtidas
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_mentions_post_email %}
Desabilitar e-mail de menções em posts
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_message_email %}
Desabilitar e-mail de mensagens
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_profile_post_email %}
Desabilitar e-mail de posts no perfil
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_disable_share_post_email %}
Desabilitar e-mail de compartilhamento de posts
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_enable_auto_sub_for_feeds %}
Habilitar assinatura automática de feeds
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_event_reminders_checkbox_default %}
Checkbox padrão de lembretes de eventos.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_exclude_mail_app_attachments %}
Excluir anexos do aplicativo de e-mail.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_favorites_show_top_favorites %}
Exibir principais favoritos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_favorites_wt_shown %}
Tutorial de favoritos exibido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_global_nav_bar_wt_shown %}
Tutorial barra de navegação global exibido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_global_nav_grid_menu_wt_shown %}
Tutorial menu de grade de navegação global exibido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_has_celebration_badge %}
Possui emblema de celebração
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_has_sent_warning_email %}
E-mail de aviso enviado
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_has_sent_warning_email238 %}
E-mail de aviso 238 enviado
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_has_sent_warning_email240 %}
E-mail de aviso 240 enviado
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_bigger_photo_callout %}
Ocultar aviso de foto ampliada
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_browse_product_redirect_confirmation %}
Ocultar confirmação de redirecionamento de produto
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_chatter_onboarding_splash %}
Ocultar tela de boas-vindas do Chatter
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_csn_desktop_task %}
Ocultar tarefa CSN desktop
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_csn_get_chatter_mobile_task %}
Ocultar tarefa CSN Chatter mobile
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_end_user_onboarding_assistant_modal %}
Ocultar modal de assistente de onboarding
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_lightning_migration_modal %}
Ocultar modal de migração Lightning
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_online_sales_app_tab_visibility_requirements_modal %}
Ocultar modal de requisitos de visibilidade de abas do app de vendas online
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_online_sales_app_welcome_mat %}
Ocultar tela inicial do app de vendas online.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_s1_browser_ui %}
Ocultar interface do navegador S1
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_second_chatter_onboarding_splash %}
Ocultar segunda tela de boas-vindas do Chatter
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_hide_sfx_welcome_mat %}
Ocultar tela inicial SFX.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_lightning_experience_preferred %}
Preferência pela experiência Lightning
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_live_agent_miaw_setup_deflection %}
Desvio de configuração LiveAgent MIAW
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_path_assistant_collapsed %}
Assistente de caminho recolhido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_preview_custom_theme %}
Visualizar tema personalizado
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_record_home_reserved_wt_shown %}
Tutorial reservado da página inicial do registro exibido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_record_home_section_collapse_wt_shown %}
Tutorial de colapso de seção da página inicial exibido
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_reminder_sound_off %}
Som de lembretes desligado
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_reverse_open_activities_view %}
Visualização reversa de atividades abertas
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_city_to_external_users %}
Exibir cidade para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_city_to_guest_users %}
Exibir cidade para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_country_to_external_users %}
Exibir país para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_country_to_guest_users %}
Exibir país para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_email_to_external_users %}
Exibir e-mail para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_email_to_guest_users %}
Exibir e-mail para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_fax_to_external_users %}
Exibir fax para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_fax_to_guest_users %}
Exibir fax para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_forecasting_change_signals %}
Exibir sinais de mudança de previsão
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_forecasting_rounded_amounts %}
Exibir valores arredondados de previsão
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_manager_to_external_users %}
Exibir gerente para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_manager_to_guest_users %}
Exibir gerente para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_mobile_phone_to_external_users %}
Exibir celular para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_mobile_phone_to_guest_users %}
Exibir celular para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_postal_code_to_external_users %}
Exibir CEP para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_postal_code_to_guest_users %}
Exibir CEP para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_profile_pic_to_guest_users %}
Exibir foto de perfil para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_state_to_external_users %}
Exibir estado para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_state_to_guest_users %}
Exibir estado para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_street_address_to_external_users %}
Exibir endereço para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_street_address_to_guest_users %}
Exibir endereço para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_title_to_external_users %}
Exibir título para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_title_to_guest_users %}
Exibir título para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_work_phone_to_external_users %}
Exibir telefone comercial para usuários externos
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_show_work_phone_to_guest_users %}
Exibir telefone comercial para usuários convidados
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_sort_feed_by_comment %}
Ordenar feed por comentário.
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_srh_override_activities %}
Substituição de atividades SRH
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_suppress_event_sfx_reminders %}
Suprimir lembretes de eventos SFX
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_suppress_task_sfx_reminders %}
Suprimir lembretes de tarefas SFX
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_task_reminders_checkbox_default %}
Checkbox padrão de lembretes de tarefas
{% enddocs %}

{% docs czrm_salesforce__col__user_preferences_user_debug_mode_pref %}
Preferência de modo de depuração do usuário
{% enddocs %}

{% docs czrm_salesforce__col__user_type %}
Tipo de usuário
{% enddocs %}

{% docs czrm_salesforce__col__username %}
Nome de usuário
{% enddocs %}

{% docs czrm_salesforce__col__validation_error_message %}
Mensagem de erro padrão exibida quando o Regex falha.
{% enddocs %}

{% docs czrm_salesforce__col__validation_regex %}
Expressão regular para validar o formato do dado inserido pelo usuário.
{% enddocs %}

{% docs czrm_salesforce__col__valores_cadastrados %}
Campo Roll-Up Summary que indica o total de valores cadastrados ativos para um determinado atributo.
{% enddocs %}

{% docs czrm_salesforce__col__value_boolean %}
Valor da resposta para atributos do tipo Checkbox (pergunta lógica Sim/Não).
{% enddocs %}

{% docs czrm_salesforce__col__value_date %}
Valor da resposta para atributos do tipo Date.
{% enddocs %}

{% docs czrm_salesforce__col__value_date_time %}
Valor da resposta para atributos do tipo DateTime.
{% enddocs %}

{% docs czrm_salesforce__col__value_display %}
Versão legível (denormalizada) da resposta, preenchida para TODOS os tipos. Permite exibir e reportar o valor sem parse (ex.: JSON do COMLURB vira lista de itens; Date vira dd/MM/yyyy).
{% enddocs %}

{% docs czrm_salesforce__col__value_json %}
Valor da resposta para atributos compostos (ex.: Customizado/materiais COMLURB). Guardado como JSON estruturado. Para exibicao use Value_Display__c.
{% enddocs %}

{% docs czrm_salesforce__col__value_long_text %}
Valor da resposta para atributos do tipo TextArea (texto longo).
{% enddocs %}

{% docs czrm_salesforce__col__value_multipicklist %}
Valor da resposta para atributos do tipo Multipicklist (selecao multipla). Armazena os valores separados por ponto e virgula.
{% enddocs %}

{% docs czrm_salesforce__col__value_number %}
Valor da resposta para atributos do tipo Number. Tipado para permitir somas e agregações em relatórios e no Data 360.
{% enddocs %}

{% docs czrm_salesforce__col__value_text %}
Valor da resposta para atributos dos tipos Text, Phone, Email e Picklist.
{% enddocs %}

{% docs czrm_salesforce__col__website %}
Website
{% enddocs %}

{% docs czrm_salesforce__col__whats_app_notification_sent %}
Indica se o fluxo de notificacao por WhatsApp ao cidadao ja foi processado para este andamento, evitando reenvios em caso de atualizacoes subsequentes do registro. Independente do Email_Notification_Sent__c.
{% enddocs %}

{% docs czrm_salesforce__col__wo_origem %}
Ordem de serviço de origem do vínculo.
{% enddocs %}

{% docs czrm_salesforce__col__wo_relacionada %}
Ordem de serviço destino do vínculo (a WO relacionada).
{% enddocs %}

{% docs czrm_salesforce__col__wo_update_event_bundle %}
Quando preenchido, indica que existe um pacote de eventos de andamento padrão para todos os serviços daquele órgão
{% enddocs %}

{% docs czrm_salesforce__col__work_order %}
Ordem de Serviço à qual a resposta do atributo pertence. Relacionamento Mestre-Detalhe: herda o compartilhamento da OS e é excluído em cascata com ela.
{% enddocs %}

{% docs czrm_salesforce__col__work_order_number %}
Número da ordem de serviço
{% enddocs %}

