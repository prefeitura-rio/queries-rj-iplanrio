# IplanRio Data Lake - DBT Models

Este repositório contém todos os modelos DBT (Data Build Tool) da IplanRio, responsáveis pela transformação e modelagem de dados em nosso Data Lake.

## Contribuindo com o projeto

Se você deseja contribuir com o projeto, acesse nossa [guia de contribuição](https://iplan-rio.mintlify.app/data-lake/dbt/colaboracao-queries).

## Sobre o DBT

O **DBT (Data Build Tool)** é uma ferramenta de transformação de dados que permite aos analistas e engenheiros de dados transformar dados em seus warehouses de forma eficiente e confiável. No contexto da IplanRio, utilizamos o DBT para:

- **Transformação de dados**: Limpeza, padronização e enriquecimento de dados brutos
- **Modelagem dimensional**: Criação de tabelas de fatos e dimensões para análise
- **Testes**: Validação de qualidade e integridade dos dados
- **Versionamento**: Controle de versão para todas as transformações de dados

## Estrutura do Projeto

O projeto segue as seguintes práticas de organização do DBT:

```
queries-rj-iplanrio/
├── 📁 models/                    # Modelos DBT (transformações de dados)
│   ├── 📁 raw/                   # Modelos de dados brutos (staging)
│   ├── 📁 core/                  # Modelos centrais de negócio (dimensões e fatos)
│   └── 📁 mart/                  # Data marts para análise específica
├── 📁 macros/                    # Macros reutilizáveis
│   ├── 📁 dbt_internal/          # Macros internos do projeto
│   ├── 📁 string/                # Macros para manipulação de strings
├── 📁 scripts/                   # Scripts de manutenção e automação
│   ├── 📁 cli/                   # Interface de linha de comando
│   ├── 📁 core/                  # Módulos de transformação
│   ├── 📁 tests/                 # Suite de testes
├── 📁 .github/                   # Configurações do GitHub
│   ├── 📁 workflows/             # Workflows de CI/CD
│   │   ├── dbt-ci.yml            # CI para Pull Requests
│   │   ├── dbt-cd.yml            # CD para produção
│   │   ├── dbt-drop-dev-schemas.yml # Limpeza de schemas de desenvolvimento
│   │   └── sqlfmt.yml            # Formatação automática de SQL
├── 📁 analyses/                  # Análises ad-hoc (consultas exploratórias)
├── 📁 seeds/                     # Dados .csv
├── 📁 snapshots/                 # Modelos de snapshot
├── 📁 tests/                     # Testes customizados de qualidade dbt
├── 📁 .cursor/                   # Configurações do Cursor IDE
├── cli.py                        # Ponto de entrada da CLI de scripts
├── dbt_project.yml               # Configuração principal do projeto DBT
├── profiles.yml                  # Configurações de conexão com banco de dados
├── packages.yml                  # Dependências de pacotes DBT
├── package-lock.yml              # Versões fixas dos pacotes
├── dbt-requirements.txt          # Dependências Python para DBT
├── pyproject.toml                # Configuração do projeto Python
├── uv.lock                       # Lock file do gerenciador de pacotes UV
├── .pre-commit-config.yaml       # Hooks de pré-commit para qualidade de código
├── .dbtignore                    # Arquivos ignorados pelo DBT
├── .gitignore                    # Arquivos ignorados pelo Git
├── .python-version               # Versão do Python para o projeto
├── recce.yml                     # Configuração da ferramenta Recce
└── README.md                     # Este arquivo de documentação
```

### **Arquivos de Configuração Principais:**

- **[`cli.py`](scripts/README.md)**: Ponto de entrada da CLI de scripts de manutenção
- **`dbt_project.yml`**: Configurações do projeto, modelos, testes e variáveis
- **`profiles.yml`**: Conexões com BigQuery e outros bancos de dados
- **`packages.yml`**: Dependências de pacotes DBT externos
- **`dbt-requirements.txt`**: Dependências Python necessárias para execução

### **Diretórios de Modelos:**

- **`models/raw/`**: Modelos que fazem staging dos dados brutos
- **`models/core/`**: Modelos centrais com lógica de negócio
- **`models/mart/`**: Data marts para análises específicas

### **Workflows de CI/CD:**

- **`dbt-ci.yml`**: Executa testes e validações em Pull Requests
- **`dbt-cd.yml`**: Deploy automático para produção após merge
- **`dbt-drop-dev-schemas.yml`**: Limpeza automática de schemas de desenvolvimento
- **`sqlfmt.yml`**: Formatação automática de código SQL

### **Ferramentas:**

- [**Scripts CLI**](scripts/README.md): Suite de automação para migracao, manutenção e transformações
- **Recce**: Ferramenta para comparação de resultados entre diferentes execuções
- **Pre-commit**: Hooks para validação automática de código antes do commit

## Comandos Básicos do DBT

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

## Scripts CLI de Manutenção

Este repositório inclui uma [**suite de scripts CLI**](scripts/README.md) para automação de tarefas de manutenção e padronização de código dbt. A CLI unificada ([`cli.py`](scripts/README.md)) facilita a execução de transformações em lote com análise inteligente de contexto.

**📚 Documentação completa:** [`scripts/README.md`](scripts/README.md)

## Recursos Adicionais

- [Documentação oficial do DBT](https://docs.getdbt.com/docs/introduction)
- [Documentação oficial do Data Lake](https://iplan-rio.mintlify.app/data-lake/overview)

---


