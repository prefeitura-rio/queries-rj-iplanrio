# DBT Scripts CLI

Sistema profissional de scripts para manutenção e transformação de código dbt.

## 🚀 Quick Start

```bash
# Executar CLI a partir da raiz do projeto
python cli.py <comando> [opções]

# Ver comandos disponíveis
python cli.py list

# Ver ajuda de um comando específico
python cli.py clean-cast --help
```

## 📂 Estrutura

```
├── cli.py                              # Ponto de entrada principal
├── scripts/
│   ├── cli/
│   │   ├── main.py                     # CLI principal
│   │   └── __init__.py
│   ├── core/                           # Módulos de transformação
│   │   ├── sql_context_analyzer.py     # Análise inteligente de contexto SQL
│   │   ├── replace_with_clean_and_cast_macro.py
│   │   ├── enforce_id_string_type.py
│   │   ├── remove_nonstring_quotes_all.py
│   │   ├── replace_ex_with_exemplo.py
│   │   └── __init__.py
│   ├── tests/                          # Bateria de testes
│   │   ├── test_runner.py              # Executor de testes
│   │   ├── fixtures/
│   │   │   ├── input/                  # Arquivos de teste (antes)
│   │   │   └── expected/               # Resultados esperados (depois)
│   │   └── __init__.py
│   └── docs/                           # Documentação detalhada
│       ├── QUICKSTART.md
│       └── README_*.md
```

## 🧪 Testes

Sistema completo de testes para validar transformações:

```bash
# Executar bateria de testes
python scripts/tests/test_runner.py

# Output esperado:
# ✅ PASS - 01_simple_source.sql
# ✅ PASS - 02_simple_ref.sql
# ✅ PASS - 03_mixed_joins.sql
# ...
# 📊 Total: 7 | ✅ Passou: 7 | ❌ Falhou: 0
```

### Casos de Teste

1. **01_simple_source.sql** - SELECT de source (aplica transformações)
2. **02_simple_ref.sql** - SELECT de ref (pula transformações)
3. **03_mixed_joins.sql** - JOIN entre source e ref (edge case)
4. **04_long_select.sql** - SELECT com 100+ colunas
5. **05_id_columns_source.sql** - Colunas id_* de source
6. **06_id_columns_ref.sql** - Colunas id_* de ref (pula)
7. **07_multiline_complex.sql** - Transformações multi-linha

## 📜 Comandos Disponíveis

### clean-cast

Substitui padrões `SAFE_CAST(REGEXP_REPLACE(...))` pelo macro `clean_and_cast`.

```bash
# Análise inteligente (padrão) - aplica apenas em sources
python cli.py clean-cast --dry-run

# Aplicar mudanças
python cli.py clean-cast --apply

# Desabilitar análise de contexto (força todas as transformações)
python cli.py clean-cast --apply --no-context-check
```

### enforce-id-string

Garante que todas as colunas `id_*` sejam do tipo STRING.

```bash
# Ver o que seria alterado
python cli.py enforce-id-string --dry-run --verbose

# Aplicar mudanças
python cli.py enforce-id-string --apply

# Processar apenas um modelo específico
python cli.py enforce-id-string --dry-run -m mart_ergon_saude_funcionarios
```

### remove-quotes

Remove `quote: true` de campos não-string em arquivos YAML.

```bash
python cli.py remove-quotes --apply
```

### fix-exemplo

Substitui `Ex:` por `Exemplo` em arquivos YAML.

```bash
python cli.py fix-exemplo --apply
```

## 🧠 Análise Inteligente de Contexto

Os scripts usam análise de contexto SQL para aplicar transformações apenas onde necessário:

| Origem | Ação | Razão |
|--------|------|-------|
| `{{ source('schema', 'table') }}` | ✅ Aplica | Dados brutos precisam de transformação |
| `{{ ref('modelo') }}` | ⏭️ Pula | Já foi transformado no modelo raw |
| `` `projeto.dataset.tabela` `` | ✅ Aplica | Tabela hardcoded precisa de transformação |

### Como funciona

1. Detecta o bloco SQL completo (CTE ou query)
2. Identifica a origem dos dados (source/ref/hardcoded)
3. Aplica transformação apenas em origens brutas
4. Evita re-processamento de dados já limpos

## 🔧 Flags Globais

| Flag | Descrição | Padrão |
|------|-----------|--------|
| `--apply` | Aplica as mudanças (desativa dry-run) | `false` |
| `--dry-run` | Mostra o que seria alterado sem modificar | `true` |
| `--path PATH` | Caminho base para processar | `models` |
| `--model`, `-m` | Nome do modelo específico | - |
| `--verbose`, `-v` | Mostra detalhes de todas as alterações | `false` |
| `--no-context-check` | Desabilita análise de contexto | `false` |

## 📖 Exemplos de Uso

### Processar pasta específica

```bash
python cli.py clean-cast --apply --path models/raw/sma
```

### Processar modelo específico

```bash
python cli.py enforce-id-string --dry-run -m raw_funcionarios
```

### Sequência completa de transformações

```bash
# 1. Backup
git add . && git commit -m "backup antes de transformações"

# 2. Testar scripts
python scripts/tests/test_runner.py

# 3. Aplicar transformações
python cli.py fix-exemplo --apply
python cli.py remove-quotes --apply
python cli.py clean-cast --apply
python cli.py enforce-id-string --apply

# 4. Validar
dbt compile

# 5. Commit
git add . && git commit -m "refactor: aplicar transformações automatizadas"
```

## 🐛 Troubleshooting

### Testes falhando

```bash
# Ver diff detalhado
python scripts/tests/test_runner.py

# Verificar arquivo específico
diff scripts/tests/fixtures/input/02_simple_ref.sql \
     scripts/tests/fixtures/expected/02_simple_ref.sql
```

### Imports não funcionando

```bash
# Executar sempre a partir da raiz
cd /Users/m/github/emd/queries-rj-iplanrio
python cli.py <comando>
```

### Contexto não detectando refs

```bash
# Verificar se sql_context_analyzer está importado
python -c "from scripts.core.sql_context_analyzer import should_apply_transformation; print('OK')"
```

## 🤝 Contribuindo

### Adicionando novo script

1. Criar módulo em `scripts/core/novo_script.py`
2. Adicionar registro em `scripts/cli/main.py`:
   ```python
   SCRIPTS = {
       "novo-comando": {
           "file": "novo_script.py",
           "description": "Breve descrição",
           ...
       }
   }
   ```
3. Criar fixture de teste em `scripts/tests/fixtures/`
4. Executar testes: `python scripts/tests/test_runner.py`

### Adicionando novo caso de teste

1. Criar `input/XX_test_name.sql` com código antes da transformação
2. Criar `expected/XX_test_name.sql` com resultado esperado
3. Executar: `python scripts/tests/test_runner.py`

## 📄 Licença

Interno - Prefeitura do Rio de Janeiro
