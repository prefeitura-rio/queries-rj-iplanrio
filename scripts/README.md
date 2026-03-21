# DBT Scripts CLI

Suite de automação para manutenção e padronização de código dbt. Centraliza transformações complexas que seriam trabalhosas manualmente, com análise inteligente de contexto SQL para evitar re-processamento de dados já transformados.

**Por que usar:**
- 🧠 Detecta automaticamente origem dos dados (`source` vs `ref`) para aplicar transformações apenas onde necessário
- 🛡️ Dry-run por padrão - revise antes de aplicar
- 🧪 Testes automatizados garantem qualidade
- 🎯 Busca flexível por path, modelo ou padrão fuzzy

## 🚀 Quick Start

```bash
# Ver comandos e ajuda
python cli.py list
python cli.py clean-cast --help

# Workflow típico
python cli.py clean-cast --dry-run              # Revisar mudanças
python cli.py clean-cast --apply --path models/raw/sma
python cli.py test                              # Validar
```

## 📂 Estrutura

```text
queries-rj-iplanrio/
├── cli.py                              # ⭐ Ponto de entrada
├── macros/clean_and_cast.sql           # Macro principal (trim=true padrão)
└── scripts/
    ├── cli/main.py                     # CLI com todos os comandos
    ├── core/                           # Módulos de transformação
    │   ├── sql_context_analyzer.py     # Análise inteligente de contexto
    │   ├── replace_with_clean_and_cast_macro.py
    │   ├── enforce_id_string_type.py
    │   └── ...
    └── tests/                          # Testes organizados por comando
        ├── test_runner.py
        ├── clean-cast/{input,expected}/
        ├── enforce-id-string/{input,expected}/
        └── integration/
```

## 📋 Comandos e Flags

### Comandos

| Comando | Descrição |
|---------|-----------|
| `list` | Lista scripts disponíveis |
| `test` | Executa suite de testes |
| `clean-cast` | Substitui SAFE_CAST por macro clean_and_cast |
| `enforce-id-string` | Garante que colunas id_* sejam STRING |
| `remove-quotes` | Remove quote: true de colunas não-string |
| `fix-exemplo` | Padroniza "Ex:" → "Exemplo" |

### Flags Globais

| Flag | Descrição | Padrão |
|------|-----------|--------|
| `--apply` | Aplica mudanças (desativa dry-run) | `false` |
| `--dry-run` | Mostra mudanças sem modificar | `true` |
| `--path PATH` | Caminho para processar | `models/` |
| `--model`, `-m` | Modelo específico (busca fuzzy) | - |
| `--verbose`, `-v` | Detalhes de todas alterações | `false` |
| `--no-context-check` | Desabilita análise de contexto | `false` |
| `--skip-yaml` | Não atualiza YAMLs | `false` |

## 📜 Detalhes dos Comandos

### clean-cast

Substitui padrões verbosos por macro `clean_and_cast`:
- `SAFE_CAST(REGEXP_REPLACE(TRIM(col),...) AS tipo)` → `{{ clean_and_cast('col', 'tipo') }}`
- `SAFE_CAST(TRIM(col) AS tipo)` → `{{ clean_and_cast('col', 'tipo') }}`
- `CAST(col AS tipo)` → `{{ clean_and_cast('col', 'tipo', safe=false) }}`

**Análise inteligente ativa:** Aplica em `{{ source() }}`, ignora `{{ ref() }}`.

```bash
python cli.py clean-cast --dry-run
python cli.py clean-cast --apply --path models/raw/sma
python cli.py clean-cast --apply --no-context-check  # Força tudo
```

### enforce-id-string

Garante tipo STRING para todas colunas `id_*`:
1. Detecta tipos incorretos (INT64, INTEGER, etc.)
2. Corrige SQL: `{{ clean_and_cast('coluna', 'string') }}`
3. Atualiza YAML: `data_type: string`

```bash
python cli.py enforce-id-string --dry-run --verbose
python cli.py enforce-id-string --apply
python cli.py enforce-id-string --apply --skip-yaml
```

### test

Valida transformações executando CLI via subprocess (não imports diretos).

```bash
python cli.py test                    # Todos
python cli.py test --filter 08        # Filtrar
python cli.py test --verbose          # Com diffs
```

## 📖 Exemplos

### Workflow Completo

```bash
# Backup → Testar → Transformar → Validar → Commit
git add . && git commit -m "backup"
python cli.py test
python cli.py clean-cast --apply
dbt compile
python cli.py test
git add . && git commit -m "refactor: padronizações"
```

### Por Pasta/Modelo

```bash
# Pasta específica
python cli.py clean-cast --apply --path models/raw/sma

# Modelo (busca fuzzy)
python cli.py clean-cast --apply --model funcionario

# Múltiplos matches → mostra lista
python cli.py clean-cast --model func
# ⚠️  Múltiplos encontrados: raw_funcionarios.sql, mart_funcionarios.sql
# 💡 Use nome completo
```

### Transformações em Sequência

```bash
python cli.py fix-exemplo --apply
python cli.py remove-quotes --apply
python cli.py clean-cast --apply
python cli.py enforce-id-string --apply
```

## 🧠 Análise de Contexto

**Problema:** Scripts "burros" transformam tudo, incluindo `{{ ref() }}` que já foram transformados nos modelos raw, gerando código redundante e queries lentas.

**Solução:** CLI detecta origem e aplica transformações apenas onde necessário:

| Origem | Ação | Razão |
|--------|------|-------|
| `{{ source(...) }}` | ✅ Aplica | Dados brutos precisam limpeza |
| `{{ ref(...) }}` | ⏭️ Pula | Já transformados no raw |
| `` `projeto.dataset.tabela` `` | ✅ Aplica | Tabela hardcoded precisa limpeza |

**Como funciona:**
1. Detecta blocos SQL (CTEs, subqueries) por profundidade de parênteses
2. Encontra FROM/JOIN no bloco
3. Identifica origem (source/ref/hardcoded)
4. Decide aplicar ou pular

**Exemplo:**
```sql
-- CTE 1: source → APLICA
WITH func AS (
  SELECT CAST(num AS INT64) AS id  -- ✅ Transformado
  FROM {{ source('ergon', 'funcionarios') }}
)
-- CTE 2: ref → PULA
, venc AS (
  SELECT CAST(id AS STRING) AS id  -- ⏭️ Ignorado
  FROM {{ ref('raw_funcionarios') }}
)
```

**Desabilitar:** Use `--no-context-check` para forçar tudo.

## 🧪 Testes

Testes executam CLI via subprocess e comparam output com expected. Organizados por comando em `scripts/tests/<comando>/{input,expected}/`.

**Cobertura:** SELECT simples, JOINs, CTEs múltiplas, subqueries, window functions (QUALIFY), ARRAY_AGG, UNION ALL, queries aninhadas.

**Adicionar teste:**
```bash
# Criar par input/expected
cat > scripts/tests/clean-cast/input/99_teste.sql
cat > scripts/tests/clean-cast/expected/99_teste.sql

# Executar (descoberta automática)
python cli.py test --filter 99
```

**Não é necessário editar código** - test runner descobre automaticamente.

## 🐛 Troubleshooting

```bash
# Testes falhando
python cli.py test --verbose  # Ver diffs
python cli.py test --filter 08

# Imports não funcionando - executar da raiz do projeto
cd <diretório-raiz-do-projeto>  # ✅
python cli.py <comando>

# Transformações não aplicadas
python cli.py clean-cast --apply  # Não esqueça --apply
python cli.py clean-cast --dry-run --verbose  # Ver por que pulou

# Modelo não encontrado
python cli.py clean-cast --model raw_recursos_humanos_ergon__funcionario  # Nome completo
python cli.py clean-cast --path models/raw/sma/recursos_humanos_ergon  # Ou path
```

## 🤝 Contribuindo

### Novo Comando

1. **Criar `scripts/core/novo_script.py`:**
```python
def process_file(file_path: Path, dry_run: bool = False) -> Tuple[int, dict]:
    # Lógica aqui
    return changes_count, details
```

2. **Adicionar em `scripts/cli/main.py`:**
```python
novo_parser = subparsers.add_parser('novo-comando', help='...')
def run_novo_comando(args):
    from core.novo_script import process_file
    # ...
```

3. **Criar testes em `scripts/tests/novo-comando/{input,expected}/`**

4. **Validar:** `python cli.py novo-comando --dry-run && python cli.py test`

### Novo Teste

```bash
# Criar par input/expected
cat > scripts/tests/clean-cast/input/14_caso.sql << 'EOF'
SELECT CAST(col AS INT64) FROM {{ source('s', 't') }}
EOF

cat > scripts/tests/clean-cast/expected/14_caso.sql << 'EOF'
SELECT {{ clean_and_cast('col', 'int64') }} FROM {{ source('s', 't') }}
EOF

# Executar
python cli.py test --filter 14
```

### Boas Práticas

- ✅ Sempre dry-run antes de apply
- ✅ Crie testes para novos comandos
- ✅ Use análise de contexto quando aplicável
- ✅ Valide com `dbt compile` após transformações
- ✅ Documente no `--help`
- ✅ Retorne métricas úteis
- ✅ Teste edge cases (refs, sources, CTEs, subqueries)

## 📄 Licença

Interno - Prefeitura do Rio de Janeiro
