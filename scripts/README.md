# DBT Scripts CLI

CLI centralizado para executar scripts de manutenção e transformação no projeto dbt queries-rj-iplanrio.

## 🚀 Quick Start

```bash
# Ver todos os scripts disponíveis
python scripts/main.py list

# Ver ajuda geral
python scripts/main.py --help

# Ver ajuda de um comando específico
python scripts/main.py clean-cast --help

# Executar em modo dry-run (padrão - não modifica arquivos)
python scripts/main.py clean-cast

# Aplicar mudanças
python scripts/main.py clean-cast --apply
```

## 📋 Scripts Disponíveis

### 1. clean-cast
Substitui padrões `SAFE_CAST(REGEXP_REPLACE(...))` pelo macro `clean_and_cast`.

**Uso:**
```bash
# Ver o que seria alterado
python scripts/main.py clean-cast --dry-run

# Aplicar em uma pasta específica
python scripts/main.py clean-cast --apply --path models/raw/sma

# Com saída detalhada
python scripts/main.py clean-cast --verbose
```

**O que faz:**
- Detecta padrões como `SAFE_CAST(REGEXP_REPLACE(coluna, r'\.0$', '') AS tipo)`
- Substitui por `{{ clean_and_cast('coluna', 'tipo') }}`
- Suporta 6 variações do padrão, incluindo multi-linha
- Processa arquivos `.sql`

---

### 2. remove-quotes
Remove `quote: true` de campos não-string em arquivos YAML.

**Uso:**
```bash
# Ver o que seria alterado
python scripts/main.py remove-quotes --dry-run

# Aplicar em todo o projeto
python scripts/main.py remove-quotes --apply

# Apenas em uma pasta
python scripts/main.py remove-quotes --apply --path models/raw/pgm
```

**O que faz:**
- Remove propriedade `quote: true` de colunas que não são do tipo `string`
- Evita erros de sintaxe no dbt
- Processa arquivos `.yml`

---

### 3. fix-exemplo
Substitui abreviações "Ex:" por "Exemplo" em arquivos YAML.

**Uso:**
```bash
# Ver o que seria alterado
python scripts/main.py fix-exemplo --dry-run

# Aplicar mudanças
python scripts/main.py fix-exemplo --apply
```

**O que faz:**
- Substitui `Ex:` ou `Ex.:` por `Exemplo `
- Padroniza descrições em schemas YAML
- Processa arquivos `.yml`

---

## ⚙️ Flags Globais

| Flag | Descrição | Padrão |
|------|-----------|--------|
| `--apply` | Aplica as mudanças (desativa dry-run) | `false` |
| `--dry-run` | Mostra o que seria alterado sem modificar | `true` |
| `--path PATH` | Caminho base para processar | `models` |
| `--verbose`, `-v` | Mostra detalhes de todas as alterações | `false` |
| `--file-pattern` | Padrão glob para filtrar arquivos | Varia por script |
| `--help`, `-h` | Mostra ajuda | - |

## 📖 Exemplos de Uso

### Exemplo 1: Verificar antes de aplicar
```bash
# Sempre rode em dry-run primeiro
python scripts/main.py clean-cast --dry-run --verbose

# Se estiver OK, aplique
python scripts/main.py clean-cast --apply
```

### Exemplo 2: Processar apenas uma pasta
```bash
python scripts/main.py clean-cast --apply --path models/raw/sma/recursos_humanos_ergon
```

### Exemplo 3: Processar vários scripts em sequência
```bash
# Corrigir exemplos primeiro
python scripts/main.py fix-exemplo --apply

# Depois limpar quotes
python scripts/main.py remove-quotes --apply

# Por fim, aplicar macros
python scripts/main.py clean-cast --apply
```

### Exemplo 4: Filtrar por padrão de arquivo
```bash
# Apenas arquivos com "ergon" no nome
python scripts/main.py clean-cast --path models/raw/sma --file-pattern "*ergon*.sql"
```

## 🔒 Segurança

- **Dry-run por padrão**: Todos os scripts rodam em modo simulação por padrão
- **Flag --apply explícita**: É necessário passar `--apply` para modificar arquivos
- **Backup recomendado**: Faça commit do código antes de executar com `--apply`
- **Validação**: Execute `dbt compile` após mudanças para validar

## 🛠️ Fluxo de Trabalho Recomendado

1. **Fazer commit do código atual**
   ```bash
   git add .
   git commit -m "feat: antes de executar scripts"
   ```

2. **Testar em dry-run**
   ```bash
   python scripts/main.py <comando> --dry-run --verbose
   ```

3. **Aplicar em uma pasta pequena primeiro**
   ```bash
   python scripts/main.py <comando> --apply --path models/raw/sma/recursos_humanos_ergon
   ```

4. **Validar**
   ```bash
   dbt compile --select raw_recursos_humanos_ergon
   ```

5. **Se OK, aplicar em todo o projeto**
   ```bash
   python scripts/main.py <comando> --apply
   ```

6. **Validar tudo**
   ```bash
   dbt compile
   ```

7. **Fazer commit**
   ```bash
   git add .
   git commit -m "refactor: aplicar macro clean_and_cast"
   ```

## 📂 Estrutura de Scripts

```
scripts/
├── main.py                                    # CLI principal
├── README.md                                  # Esta documentação
├── README_clean_and_cast.md                   # Docs específicas do clean_and_cast
├── replace_with_clean_and_cast_macro.py       # Script do clean-cast
├── remove_nonstring_quotes_all.py             # Script do remove-quotes
└── replace_ex_with_exemplo.py                 # Script do fix-exemplo
```

## 🆕 Adicionando Novos Scripts

Para adicionar um novo script ao CLI:

1. **Criar o script** em `/scripts/seu_script.py`

2. **Adicionar ao registro** em `main.py`:
   ```python
   SCRIPTS = {
       "seu-comando": {
           "file": "seu_script.py",
           "description": "Breve descrição",
           "long_description": "Descrição detalhada",
           "default_path": "models",
           "supports": ["--path", "--verbose", "--apply"],
           "examples": [
               "python scripts/main.py seu-comando --dry-run",
           ]
       },
   }
   ```

3. **Criar o subparser** em `create_parser()`:
   ```python
   seu_parser = subparsers.add_parser("seu-comando", ...)
   add_common_args(seu_parser, "seu-comando")
   ```

4. **Criar a função runner** `run_seu_comando(args)`

5. **Adicionar ao main()**: `if args.command == "seu-comando": return run_seu_comando(args)`

## 🐛 Troubleshooting

### Erro: "Caminho não encontrado"
```bash
# Certifique-se de executar do diretório raiz do projeto
cd /Users/m/github/emd/queries-rj-iplanrio
python scripts/main.py <comando>
```

### Erro: "Module not found"
```bash
# O script adiciona o diretório scripts ao path automaticamente
# Se ainda der erro, verifique se o arquivo do script existe
ls -la scripts/
```

### Erro: dbt não compila após mudanças
```bash
# Reverta as mudanças e reporte o erro
git diff
git checkout .

# Ou aplique em uma pasta pequena primeiro para isolar o problema
python scripts/main.py <comando> --apply --path models/raw/sma/recursos_humanos_ergon
```

### Nenhuma mudança detectada
```bash
# Use --verbose para ver mais detalhes
python scripts/main.py <comando> --dry-run --verbose

# Verifique se está no caminho certo
python scripts/main.py <comando> --path models/raw/sma
```

## 📊 Output Esperado

```
    ╔════════════════════════════════════════════════╗
    ║         DBT Scripts CLI - queries-rj           ║
    ║   Ferramentas de manutenção e transformação    ║
    ╚════════════════════════════════════════════════╝

🔧 Executando: clean-cast
📁 Caminho: models/raw/sma
🔒 Modo: DRY-RUN (simulação)

📄 Encontrados 30 arquivos SQL

⚠️  MODO DRY-RUN - Nenhum arquivo será modificado

🔍 models/raw/sma/recursos_humanos_ergon/raw_recursos_humanos_ergon__funcionario.sql
  5 substituição(ões)

================================================================================
📊 RESUMO:
  Arquivos processados: 30
  Arquivos modificados: 8
  Total de substituições: 63

💡 Execute com --apply para aplicar as mudanças
```

## 📝 Notas

- Todos os scripts usam UTF-8 encoding
- Preservam indentação original
- São case-insensitive quando apropriado
- Processam arquivos recursivamente no caminho especificado
- Mostram progresso e resumo ao final

## 🤝 Contribuindo

Ao adicionar novos scripts:
1. Mantenha o padrão dry-run por padrão
2. Forneça saída clara e informativa
3. Adicione exemplos na documentação
4. Teste com `--dry-run` e `--verbose` antes de commit
5. Atualize este README

## 📄 Licença

Interno - Prefeitura do Rio de Janeiro
