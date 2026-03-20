# Script: Replace with clean_and_cast Macro

Script Python para substituir automaticamente padrões de `SAFE_CAST(REGEXP_REPLACE(...))` pelo macro `clean_and_cast` em arquivos SQL do dbt.

## Pré-requisitos

- Python 3.6+
- Nenhuma dependência externa (usa apenas biblioteca padrão)

## Uso

### 1. Modo Dry-Run (Recomendado primeiro)

Visualize o que seria alterado sem modificar os arquivos:

```bash
python scripts/replace_with_clean_and_cast_macro.py --dry-run
```

Com output detalhado:

```bash
python scripts/replace_with_clean_and_cast_macro.py --dry-run --verbose
```

### 2. Aplicar em uma pasta específica

```bash
# Apenas nos modelos de recursos_humanos_ergon
python scripts/replace_with_clean_and_cast_macro.py --path models/raw/sma/recursos_humanos_ergon

# Apenas nos modelos da PGM
python scripts/replace_with_clean_and_cast_macro.py --path models/raw/pgm
```

### 3. Aplicar em todos os modelos

```bash
python scripts/replace_with_clean_and_cast_macro.py
```

## Padrões Detectados

O script identifica e substitui os seguintes padrões:

### Padrão 1: Com TRIM e CAST
```sql
-- Antes
SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(CPF AS STRING)), r'\.0$', '') AS string)

-- Depois
{{ clean_and_cast('CPF', 'string', trim=true) }}
```

### Padrão 2: Com TRIM apenas
```sql
-- Antes
SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS int64)

-- Depois
{{ clean_and_cast('numfunc', 'int64', trim=true) }}
```

### Padrão 3: Com CAST apenas
```sql
-- Antes
SAFE_CAST(REGEXP_REPLACE(CAST(NUMERO AS STRING), r'\.0$', '') AS int64)

-- Depois
{{ clean_and_cast('NUMERO', 'int64') }}
```

### Padrão 4: Sem TRIM nem CAST
```sql
-- Antes
SAFE_CAST(REGEXP_REPLACE(matric, r'\.0$', '') AS STRING)

-- Depois
{{ clean_and_cast('matric', 'string') }}
```

### Padrão 5: CAST sem SAFE
```sql
-- Antes
CAST(REGEXP_REPLACE(CAST(valor AS STRING), r'\.0$', '') AS string)

-- Depois
{{ clean_and_cast('valor', 'string', safe=false) }}
```

## Argumentos

| Argumento | Descrição | Padrão |
|-----------|-----------|--------|
| `--dry-run` | Mostra mudanças sem aplicar | `false` |
| `--path` | Caminho base para buscar arquivos | `models` |
| `--verbose`, `-v` | Mostra todas as substituições | `false` |
| `--help`, `-h` | Mostra ajuda | - |

## Exemplo de Saída

```
🔍 Buscando arquivos .sql em: /Users/m/github/emd/queries-rj-iplanrio/models/raw/sma
📁 Encontrados 25 arquivos SQL

✅ models/raw/sma/recursos_humanos_ergon/raw_recursos_humanos_ergon__funcionario.sql
  15 substituição(ões)

✅ models/raw/sma/recursos_humanos_ergon/raw_recursos_humanos_ergon__vinculo.sql
  8 substituição(ões)

================================================================================
📊 RESUMO:
  Arquivos processados: 25
  Arquivos modificados: 12
  Total de substituições: 87

✅ Substituições aplicadas com sucesso!
```

## Fluxo de Trabalho Recomendado

1. **Teste com dry-run:**
   ```bash
   python scripts/replace_with_clean_and_cast_macro.py --dry-run --verbose
   ```

2. **Aplicar em uma pasta pequena primeiro:**
   ```bash
   python scripts/replace_with_clean_and_cast_macro.py --path models/raw/sma/recursos_humanos_ergon
   ```

3. **Verificar se funciona:**
   ```bash
   dbt compile --select raw_recursos_humanos_ergon__funcionario
   ```

4. **Se OK, aplicar em todo o projeto:**
   ```bash
   python scripts/replace_with_clean_and_cast_macro.py
   ```

5. **Testar tudo:**
   ```bash
   dbt compile
   ```

## Observações

- ✅ O script preserva a indentação original
- ✅ Case-insensitive (funciona com SAFE_CAST e safe_cast)
- ✅ Seguro: não modifica arquivos em modo dry-run
- ✅ Mostra diff antes/depois de cada mudança
- ⚠️ Sempre teste com `--dry-run` primeiro
- ⚠️ Faça commit do código antes de executar
- ⚠️ Execute `dbt compile` após as mudanças para validar

## Limitações

- Não detecta padrões que estão quebrados em múltiplas linhas
- Não detecta variações muito diferentes do padrão esperado
- Requer que o nome da coluna seja um identificador simples (sem espaços ou caracteres especiais)

## Troubleshooting

**Problema:** "Arquivo não encontrado"
```bash
# Certifique-se de executar do diretório raiz do projeto
cd /Users/m/github/emd/queries-rj-iplanrio
python scripts/replace_with_clean_and_cast_macro.py
```

**Problema:** "Encoding error"
```bash
# O script usa UTF-8, certifique-se que seus arquivos estão nesse encoding
```

**Problema:** dbt não reconhece o macro
```bash
# Certifique-se que o arquivo macros/clean_and_cast.sql existe
ls -la macros/clean_and_cast.sql
```
