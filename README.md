# IplanRio Data Lake - DBT Models

Este repositório contém todos os modelos DBT (Data Build Tool) da IplanRio, responsáveis pela transformação e modelagem de dados em nosso Data Lake.

## Sobre o DBT

O **DBT (Data Build Tool)** é uma ferramenta de transformação de dados que permite aos analistas e engenheiros de dados transformar dados em seus warehouses de forma eficiente e confiável. No contexto da IplanRio, utilizamos o DBT para:

- **Transformação de dados**: Limpeza, padronização e enriquecimento de dados brutos
- **Modelagem dimensional**: Criação de tabelas de fatos e dimensões para análise
- **Documentação**: Geração automática de documentação para todos os modelos
- **Testes**: Validação de qualidade e integridade dos dados
- **Versionamento**: Controle de versão para todas as transformações de dados

## Estrutura do Projeto

O projeto segue as seguintes práticas de organização do DBT:

```
models/
├── raw/           # Modelos de dados brutos
├── core/          # Modelos centrais de negócio
└── mart/          # Data marts para análise
```

## Workflows de CI/CD

### CI (Continuous Integration) - `dbt-ci.yml`

O workflow de CI é executado automaticamente em cada Pull Request para a branch `master`:

1. **Validação**: Executa `dbt debug` para verificar configurações
2. **Dependências**: Instala dependências com `dbt deps`
3. **Build Incremental**: Executa apenas modelos modificados usando `dbt build -s 'state:modified+'`
4. **Ambiente**: Cria um dataset único no BigQuery para cada execução do workflow

### CD (Continuous Deployment) - `dbt-cd.yml`

O workflow de CD é executado automaticamente após merge na branch `master`:

1. **Deploy**: Executa todos os modelos com `dbt build`
2. **Ambiente**: Utiliza o target `pr_prod` (Produção)
3. **Manifest**: Atualiza o manifest.json no Google Cloud Storage para futuras execuções incrementais

## Instalação e Configuração

Para instalar e configurar o DBT em seu ambiente local, consulte nossa documentação completa:

📚 **[Documentação IplanRio - Instalação DBT](https://iplan-rio.mintlify.app/data-lake/dbt/instalacao-dbt)**

## Comandos Básicos

```bash
# Instalar dependências
dbt deps

# Executar todos os modelos
dbt run

# Executar testes
dbt test

# Executar testes + modelos
dbt build

# Executar apenas alguns modelos
dbt run --select 'seu_modelo'
dbt build --select 'seu_modelo'

# Debug da configuração
dbt debug
```

## Contribuição

1. Crie uma branch a partir de `master`
2. Desenvolva seus modelos seguindo as convenções do projeto
3. Execute testes localmente antes do commit
4. Abra um Pull Request para revisão
5. O CI será executado automaticamente para validação

## Recursos Adicionais

- [Documentação oficial do DBT](https://docs.getdbt.com/)
- [Comunidade DBT](https://community.getdbt.com/)
- [Discourse DBT](https://discourse.getdbt.com/)
- [Blog DBT](https://blog.getdbt.com/)

---


