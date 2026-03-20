# DBT Scripts CLI

Suite profissional de automação para manutenção, padronização e refatoração de código dbt. Esta CLI centraliza transformações complexas que seriam trabalhosas ou propensas a erro se feitas manualmente, aplicando análise inteligente de contexto SQL para garantir que as mudanças sejam aplicadas apenas onde necessário.

**Por que usar esta CLI?**

Ao invés de buscar e substituir manualmente padrões em centenas de arquivos SQL (arriscando quebrar queries que dependem de transformações já aplicadas), esta suite automatiza o processo com segurança:

- 🧠 **Análise Inteligente de Contexto**: Detecta automaticamente se dados vêm de `{{ source() }}` (precisam transformação) ou `{{ ref() }}` (já transformados), evitando re-processamento
- 🛡️ **Modo Seguro por Padrão**: Dry-run mostra todas as mudanças antes de aplicar, permitindo revisão completa
- 🧪 **Qualidade Garantida**: Suite de testes automatizados valida transformações em casos simples e complexos (CTEs, JOINs, subqueries, window functions)
- 📦 **Arquitetura Modular**: Adicione novos comandos facilmente seguindo padrões estabelecidos
- 🎯 **Busca Flexível**: Processe arquivos por path, modelo específico ou padrão fuzzy
- 📝 **Sincronização Automática**: Atualiza YAMLs automaticamente quando necessário

## 🚀 Quick Start

```bash
# Ver comandos disponíveis
python cli.py list

# Workflow típico: dry-run → revisar → aplicar → testar
python cli.py clean-cast --dry-run
python cli.py clean-cast --apply --path models/raw/sma
python cli.py test

# Ver ajuda detalhada de qualquer comando
python cli.py clean-cast --help
```

## 📂 Estrutura

```
queries-rj-iplanrio/
├── cli.py                              # ⭐ Ponto de entrada principal
├── macros/
│   └── clean_and_cast.sql              # Macro principal (trim=true padrão)
└── scripts/
    ├── cli/
    │   ├── main.py                     # CLI principal com todos os comandos
    │   └── __init__.py
    ├── core/                           # Módulos de transformação
    │   ├── sql_context_analyzer.py     # Análise inteligente de contexto SQL
    │   ├── replace_with_clean_and_cast_macro.py
    │   ├── enforce_id_string_type.py
    │   ├── remove_nonstring_quotes_all.py
    │   ├── replace_ex_with_exemplo.py
    │   └── __init__.py
    └── tests/                          # Bateria de testes organizados por comando
        ├── test_runner.py              # Executor que testa via CLI real
        ├── clean-cast/                 # Testes do comando clean-cast
        │   ├── input/                  # Casos de teste (antes)
        │   └── expected/               # Resultados esperados (depois)
        ├── enforce-id-string/          # Testes do enforce-id-string
        │   ├── input/
        │   └── expected/
        └── integration/                # Testes de integração (futuro)
```

## 📋 Resumo de Opções

Todos os comandos seguem o padrão:

```bash
python cli.py <comando> [--dry-run|--apply] [--path PATH] [--model MODEL] [opções]
```

**Comandos disponíveis:**
- `list` - Lista todos os scripts disponíveis
- `test` - Executa suite de testes automatizados
- `clean-cast` - Substitui SAFE_CAST por macro clean_and_cast
- `enforce-id-string` - Garante que colunas id_* sejam STRING
- `remove-quotes` - Remove quote: true de colunas não-string
- `fix-exemplo` - Padroniza "Ex:" para "Exemplo"

**Modos de execução:**
- **Dry-run** (padrão): Mostra mudanças sem modificar arquivos
- **Apply**: Aplica transformações nos arquivos

## 🔧 Flags Globais

Aplicáveis à maioria dos comandos (exceto `list` e `test`):

| Flag | Descrição | Padrão |
|------|-----------|--------|
| `--apply` | Aplica as mudanças nos arquivos | `false` |
| `--dry-run` | Mostra o que seria alterado (modo seguro) | `true` |
| `--path PATH` | Caminho base para processar | `models/` |
| `--model`, `-m MODEL` | Nome do modelo específico (busca fuzzy) | - |
| `--verbose`, `-v` | Mostra detalhes de todas as alterações | `false` |
| `--no-context-check` | Desabilita análise de contexto SQL | `false` |
| `--skip-yaml` | Não atualiza arquivos YAML | `false` |

## 📜 Comandos Disponíveis

### `list`

Lista todos os comandos disponíveis com descrições.

```bash
python cli.py list
```

---

### `test`

Executa bateria completa de testes que valida transformações via CLI real (subprocess).

```bash
python cli.py test                    # Todos os testes
python cli.py test --filter 08        # Filtrar por nome/número
python cli.py test --verbose          # Modo detalhado
```

**Output:**
```
✅ clean-cast         - 11/11 testes passaram
✅ enforce-id-string  - 2/2 testes passaram
```

---

### `clean-cast`

Substitui padrões verbosos de `SAFE_CAST(REGEXP_REPLACE(...))` pelo macro `clean_and_cast`.

**Padrões detectados:**
- `SAFE_CAST(REGEXP_REPLACE(TRIM(col), ...) AS tipo)` → `{{ clean_and_cast('col', 'tipo') }}`
- `SAFE_CAST(TRIM(col) AS tipo)` → `{{ clean_and_cast('col', 'tipo') }}`
- `CAST(col AS tipo)` → `{{ clean_and_cast('col', 'tipo', safe=false) }}`

**Análise inteligente:** Aplica apenas em `{{ source() }}`, ignora `{{ ref() }}` (já transformados).

```bash
python cli.py clean-cast --dry-run
python cli.py clean-cast --apply --path models/raw/sma
python cli.py clean-cast --apply --model funcionario
python cli.py clean-cast --apply --no-context-check  # Força todas transformações
```

---

### `enforce-id-string`

Garante que todas as colunas `id_*` sejam do tipo STRING, tanto nos SQLs quanto nos YAMLs correspondentes.

**O que faz:**
1. Detecta colunas `id_*` com tipos incorretos (INT64, INTEGER, etc.)
2. Corrige para `{{ clean_and_cast('coluna', 'string') }}`
3. Atualiza `data_type: string` nos arquivos YAML

```bash
python cli.py enforce-id-string --dry-run --verbose
python cli.py enforce-id-string --apply
python cli.py enforce-id-string --apply --model mart_funcionarios
python cli.py enforce-id-string --apply --skip-yaml  # Não atualiza YAMLs
```

---

### `remove-quotes`

Remove `quote: true` de colunas não-string em arquivos YAML (boas práticas dbt).

```bash
python cli.py remove-quotes --apply --verbose
python cli.py remove-quotes --dry-run --path models/mart
```

---

### `fix-exemplo`

Substitui abreviações `Ex:` por `Exemplo` nas descrições dos YAMLs para melhor legibilidade.

```bash
python cli.py fix-exemplo --apply
python cli.py fix-exemplo --dry-run --path models/core
```

## 📖 Exemplos de Uso

### Workflow Recomendado

```bash
# 1. Ver comandos disponíveis
python cli.py list

# 2. Executar testes antes de qualquer coisa
python cli.py test

# 3. Dry-run para ver impacto
python cli.py clean-cast --dry-run --path models/raw/sma

# 4. Aplicar se estiver OK
python cli.py clean-cast --apply --path models/raw/sma

# 5. Validar com dbt
dbt compile

# 6. Re-executar testes
python cli.py test
```

### Processar Pasta Específica

```bash
python cli.py clean-cast --apply --path models/raw/sma
python cli.py enforce-id-string --apply --path models/mart
```

### Processar Modelo Específico

```bash
# Busca fuzzy - encontra por nome parcial
python cli.py clean-cast --dry-run --model funcionario
python cli.py enforce-id-string --apply --model ergon_saude_funcionarios

# Se múltiplos matches, mostra lista e pede nome completo
python cli.py clean-cast --model func
# ⚠️  Múltiplos modelos encontrados para 'func':
#    - raw/ergon/raw_funcionarios.sql
#    - mart/mart_funcionarios.sql
# 💡 Use o nome completo do modelo
```

### Sequência Completa de Transformações

```bash
# 1. Backup
git add . && git commit -m "backup antes de transformações"

# 2. Testar scripts
python cli.py test

# 3. Aplicar transformações em ordem
python cli.py fix-exemplo --apply
python cli.py remove-quotes --apply
python cli.py clean-cast --apply
python cli.py enforce-id-string --apply

# 4. Validar com dbt
dbt compile

# 5. Re-testar
python cli.py test

# 6. Commit
git add . && git commit -m "refactor: aplicar padronizações automatizadas"
```

### Processar Apenas um Arquivo/Diretório

```bash
# Path completo
python cli.py clean-cast --apply --path models/raw/sma/recursos_humanos_ergon

# Path relativo também funciona
python cli.py clean-cast --apply --path models/mart
```

## 🧪 Sistema de Testes

Sistema completo de testes que valida transformações executando a **CLI real via subprocess** (não imports diretos dos módulos). Isso garante que os testes validam a integração completa, incluindo parsing de argumentos, execução da CLI e output final.

### Como Funciona

Os testes seguem uma estrutura simples:

1. **Organização por Comando**: Cada comando tem seu diretório em `scripts/tests/<comando>/`
2. **Pares Input/Expected**: Para cada teste, existe um arquivo `input/XX_nome.sql` (antes) e `expected/XX_nome.sql` (depois)
3. **Descoberta Automática**: O test runner encontra automaticamente todos os pares de arquivos
4. **Execução via CLI**: Cada teste executa a CLI real: `python cli.py <comando> --apply <arquivo>`
5. **Comparação**: Compara o output gerado com o expected, normalizando whitespace

### Estrutura

```
scripts/tests/
├── test_runner.py              # Executor principal
├── clean-cast/
│   ├── input/                  # SQLs antes da transformação
│   │   ├── 01_simple.sql
│   │   ├── 02_complex.sql
│   │   └── ...
│   └── expected/               # SQLs após transformação (resultado esperado)
│       ├── 01_simple.sql
│       ├── 02_complex.sql
│       └── ...
├── enforce-id-string/
│   ├── input/
│   └── expected/
└── integration/                # Testes de integração (futuro)
```

### Executar Testes

```bash
# Todos os testes
python cli.py test

# Filtrar por padrão
python cli.py test --filter 08
python cli.py test --filter real_case

# Modo verbose (mostra diffs)
python cli.py test --verbose
```

### Cobertura

Os testes cobrem casos progressivamente complexos:
- ✅ SELECT simples de source e ref
- ✅ JOINs entre sources e refs (edge cases)
- ✅ CTEs múltiplas com transformações
- ✅ Subqueries no FROM
- ✅ Window functions (QUALIFY, ROW_NUMBER, PARTITION BY)
- ✅ Funções de agregação (ARRAY_AGG, STRUCT)
- ✅ UNION ALL combinando sources e refs
- ✅ Queries aninhadas em múltiplos níveis
- ✅ Transformações multi-linha com REGEXP_REPLACE

### Adicionar Novo Teste

Basta criar o par de arquivos:

```bash
# 1. Criar input (SQL antes da transformação)
cat > scripts/tests/clean-cast/input/99_meu_teste.sql << 'EOF'
SELECT
  SAFE_CAST(coluna AS INT64) AS valor
FROM {{ source('schema', 'table') }}
EOF

# 2. Criar expected (SQL esperado após transformação)
cat > scripts/tests/clean-cast/expected/99_meu_teste.sql << 'EOF'
SELECT
  {{ clean_and_cast('coluna', 'int64') }} AS valor
FROM {{ source('schema', 'table') }}
EOF

# 3. Executar
python cli.py test --filter 99
```

O test runner descobre e executa automaticamente - **não é necessário editar código**.

## 🧠 Análise Inteligente de Contexto

A feature mais importante desta CLI é a **análise de contexto SQL**, que evita re-processar dados já transformados.

### Por Que Isso Importa?

Em um pipeline dbt típico:
- **Raw models** leem de `{{ source() }}` e aplicam transformações (CAST, TRIM, etc.)
- **Mart models** leem de `{{ ref('raw_model') }}` e apenas agregam/juntam dados

Se você rodar um script de substituição "burro" que simplesmente busca todos os `CAST(col AS tipo)`, ele vai transformar **TODOS** os arquivos, incluindo marts que referenciam dados já limpos. Isso resulta em:
- ❌ Código redundante: `TRIM(TRIM(coluna))`
- ❌ Performance pior: transformações desnecessárias
- ❌ Queries quebradas: re-casting tipos já corretos

### Como a CLI Resolve Isso

| Origem dos Dados | Ação da CLI | Razão |
|------------------|-------------|-------|
| `{{ source('schema', 'table') }}` | ✅ Aplica transformação | Dados brutos precisam de limpeza |
| `{{ ref('modelo') }}` | ⏭️ Pula transformação | Dados já foram limpos no modelo raw |
| `` `projeto.dataset.tabela` `` | ✅ Aplica transformação | Tabela hardcoded precisa de limpeza |

### Como Funciona (Técnico)

1. **Detecta blocos SQL dinamicamente** usando contador de profundidade de parênteses (não limites fixos de linhas)
2. **Encontra FROM/JOIN** dentro do bloco relevante (CTE ou query principal)
3. **Identifica origem** usando regex para detectar `{{ source() }}`, `{{ ref() }}` ou tabelas hardcoded
4. **Decide aplicar ou pular** baseado na origem
5. **Registra motivo** para auditoria (visível com `--verbose`)

### Exemplo Prático

```sql
-- CTE 1: source - APLICA transformação
WITH funcionarios AS (
  SELECT
    SAFE_CAST(numfunc AS INT64) AS id_funcionario  -- ✅ Transformado
  FROM {{ source('ergon', 'funcionarios') }}
)

-- CTE 2: ref - PULA transformação
, vencimentos AS (
  SELECT
    CAST(id_funcionario AS STRING) AS id_funcionario  -- ⏭️ Ignorado (já é string no raw)
  FROM {{ ref('raw_funcionarios') }}
)

-- Query final
SELECT
  f.id_funcionario,
  v.valor
FROM funcionarios f
LEFT JOIN vencimentos v ON f.id_funcionario = v.id_funcionario
```

**Resultado:** Transformação aplicada apenas na CTE `funcionarios`, ignorada na CTE `vencimentos`.

### Desabilitar Análise

Em casos raros onde você quer forçar transformação em **todos** os arquivos:

```bash
python cli.py clean-cast --apply --no-context-check
```

**Atenção:** Use com cuidado. Isso pode gerar código redundante em marts.

## 🐛 Troubleshooting

### Testes Falhando

```bash
# Ver output detalhado com diff
python cli.py test --verbose

# Testar apenas um caso específico
python cli.py test --filter 08

# Ver diff manual de um teste
diff scripts/tests/clean-cast/input/02_simple_ref.sql \
     scripts/tests/clean-cast/expected/02_simple_ref.sql
```

### Imports Não Funcionando

A CLI deve ser executada **sempre da raiz do projeto**:

```bash
# ✅ Correto
cd /Users/m/github/emd/queries-rj-iplanrio
python cli.py <comando>

# ❌ Errado - não vai funcionar
cd scripts/
python cli/main.py
```

### Contexto Não Detectando Refs

```bash
# Verificar se sql_context_analyzer está acessível
python -c "from scripts.core.sql_context_analyzer import should_apply_transformation; print('✅ OK')"

# Ver log do que está sendo detectado
python cli.py clean-cast --dry-run --verbose
```

### Modelo Não Encontrado

```bash
# Use nome completo se busca parcial retornar múltiplos
python cli.py clean-cast --model raw_recursos_humanos_ergon__funcionario

# Ou use path direto
python cli.py clean-cast --path models/raw/sma/recursos_humanos_ergon
```

### Transformações Não Sendo Aplicadas

```bash
# Verificar se está usando --apply (não --dry-run)
python cli.py clean-cast --apply  # ✅ Correto
python cli.py clean-cast          # ❌ Dry-run por padrão, não modifica

# Verificar se análise de contexto está bloqueando
python cli.py clean-cast --dry-run --verbose  # Mostra razão de cada skip
python cli.py clean-cast --apply --no-context-check  # Força aplicação
```

### Erros de Encoding

Se encontrar erros de encoding (raros):

```bash
# A CLI já usa UTF-8 por padrão, mas pode verificar:
python cli.py clean-cast --apply --verbose
```

## 🤝 Contribuindo

### Adicionando Novo Comando

1. **Criar módulo em `scripts/core/novo_script.py`:**

```python
from pathlib import Path
from typing import Tuple

def process_file(file_path: Path, dry_run: bool = False) -> Tuple[int, dict]:
    """
    Processa um arquivo aplicando transformações.

    Args:
        file_path: Caminho do arquivo SQL
        dry_run: Se True, não modifica arquivo

    Returns:
        Tuple (changes_count, details_dict)
    """
    # Sua lógica aqui
    changes_made = 0
    details = {}

    # ... implementação ...

    return changes_made, details
```

2. **Adicionar comando em `scripts/cli/main.py`:**

```python
# Criar parser
novo_parser = subparsers.add_parser(
    'novo-comando',
    help='Breve descrição',
    description='Descrição detalhada'
)
novo_parser.add_argument('--custom-flag', help='Flag customizada')

# Implementar função run
def run_novo_comando(args):
    from core.novo_script import process_file

    print_banner()

    # Lógica de execução (similar a outros comandos)
    # ...

    return 0 if success else 1
```

3. **Criar testes em `scripts/tests/novo-comando/`:**

```bash
mkdir -p scripts/tests/novo-comando/{input,expected}

# Criar casos de teste
cat > scripts/tests/novo-comando/input/01_basico.sql
cat > scripts/tests/novo-comando/expected/01_basico.sql
```

4. **Validar:**

```bash
python cli.py novo-comando --dry-run
python cli.py test --filter novo
```

### Boas Práticas

- ✅ **Sempre** use `--dry-run` primeiro ao testar
- ✅ **Sempre** crie testes para novos comandos/features
- ✅ Use análise de contexto (`sql_context_analyzer`) quando aplicável
- ✅ Valide mudanças com `dbt compile` após transformações SQL
- ✅ Documente flags e comportamento no `--help` do comando
- ✅ Retorne métricas úteis (arquivos processados, mudanças aplicadas)
- ✅ Adicione casos de teste progressivamente complexos
- ✅ Teste edge cases (refs, sources, hardcoded tables, CTEs, subqueries)
- ✅ Normalize whitespace em comparações de teste
- ✅ Use subprocess (não imports diretos) em testes para validar integração completa


