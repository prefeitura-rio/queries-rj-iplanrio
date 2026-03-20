# Quick Start - DBT Scripts CLI

## 🚦 Guia Rápido de 5 Minutos

### 1. Ver todos os scripts disponíveis
```bash
python scripts/main.py list
```

### 2. Testar um script (dry-run)
```bash
# Clean-cast: macro para transformações
python scripts/main.py clean-cast --dry-run

# Remove-quotes: limpar YAML
python scripts/main.py remove-quotes --dry-run

# Fix-exemplo: padronizar descrições
python scripts/main.py fix-exemplo --dry-run
```

### 3. Ver detalhes de um script
```bash
python scripts/main.py clean-cast --help
```

### 4. Aplicar mudanças
```bash
# ⚠️ SEMPRE faça commit antes!
git add .
git commit -m "feat: backup antes de scripts"

# Aplicar em uma pasta específica primeiro
python scripts/main.py clean-cast --apply --path models/raw/sma/recursos_humanos_ergon

# Validar
dbt compile --select raw_recursos_humanos_ergon

# Se OK, aplicar em tudo
python scripts/main.py clean-cast --apply
```

## 📝 Comandos Mais Usados

```bash
# Ver opções sem executar nada
python scripts/main.py

# Listar scripts
python scripts/main.py list

# Dry-run com detalhes
python scripts/main.py clean-cast --dry-run --verbose

# Aplicar em caminho específico
python scripts/main.py clean-cast --apply --path models/raw/sma

# Processar apenas certos arquivos
python scripts/main.py clean-cast --apply --file-pattern "*ergon*.sql"
```

## 🎯 Casos de Uso Comuns

### Caso 1: Padronizar transformações SQL
```bash
# 1. Ver o que seria alterado
python scripts/main.py clean-cast --dry-run --verbose

# 2. Aplicar
python scripts/main.py clean-cast --apply

# 3. Validar
dbt compile
```

### Caso 2: Limpar schemas YAML
```bash
# Remover quotes incorretas
python scripts/main.py remove-quotes --apply

# Padronizar descrições
python scripts/main.py fix-exemplo --apply
```

### Caso 3: Processar apenas uma pasta
```bash
python scripts/main.py clean-cast --apply --path models/raw/pgm
```

## ⚠️ Cuidados Importantes

1. **Sempre use dry-run primeiro**
   ```bash
   python scripts/main.py <comando> --dry-run
   ```

2. **Faça backup (commit)**
   ```bash
   git add . && git commit -m "backup"
   ```

3. **Valide após mudanças**
   ```bash
   dbt compile
   ```

4. **Teste em pasta pequena primeiro**
   ```bash
   python scripts/main.py <comando> --apply --path models/raw/sma/recursos_humanos_ergon
   ```

## 🔍 Troubleshooting Rápido

**Nada acontece?**
```bash
# Verifique se está no diretório certo
pwd  # deve estar em /Users/m/github/emd/queries-rj-iplanrio

# Veja o que seria alterado
python scripts/main.py <comando> --dry-run --verbose
```

**Erro "caminho não encontrado"?**
```bash
# Use caminho relativo a partir da raiz do projeto
python scripts/main.py <comando> --path models/raw/sma
```

**Mudanças não foram aplicadas?**
```bash
# Certifique-se de usar --apply
python scripts/main.py <comando> --apply
```

## 📚 Mais Informações

- README completo: `scripts/README.md`
- Docs do clean-cast: `scripts/README_clean_and_cast.md`
- Ajuda inline: `python scripts/main.py <comando> --help`
