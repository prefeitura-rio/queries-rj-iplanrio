# Changelog

Todas as mudanças notáveis nos modelos `sme/frequencia` são documentadas neste arquivo.

> O formato utilizado para documentação segue as boas práticas de [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/).

### 🔧 Correção

#### Commit: `5d5de0c` - 22/10/2026
**fix: corrige duplicação coc fechado + remove eja (temporário)**

Correção da lógica de acumulação de frequência para evitar duplicatas entre COCs fechados e abertos, além de tratamento temporário de alunos EJA.

**Arquivos modificados:**
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`

**Mudanças aplicadas:**
1. **Renomeação de CTE**
   - `freq_coc_atual` → `frequencia_coc_aberto`
   - Melhora na clareza e padronização de nomenclatura

2. **Refatoração da CTE `frequencia_acumulada_dias_letivos`**
   - Antes: UNION ALL direto entre `frequencia_cocs_fechados` e `frequencia_coc_aberto`
   - Depois: Adicionado LEFT JOIN para filtrar apenas registros de `frequencia_coc_aberto` que NÃO existem em `frequencia_cocs_fechados`
   - Chaves de junção: `alu_id`, `tpc_id`, `ano_calendario`
   - Cláusula: `WHERE fcf.alu_id is null`
   - Objetivo: Eliminar duplicatas entre períodos fechados e abertos

3. **Inclusão de filtro temporário para EJA**
   - Adicionada junção com `vw_bi_aluno` para filtrar alunos EJA
   - Exclusão de registros onde `Grupamento LIKE 'EJA%'` ou `Grupamento LIKE 'PROEJA%'`
   - Status: **TEMPORÁRIO** - Requer tratamento posterior para estes grupos

**Impacto:**
- Elimina duplicação de dados de frequência entre COCs fechados e abertos
- Garante que cada aluno/tipo de calendário/ano é contado apenas uma vez
- Melhora na integridade dos dados de frequência acumulada
- **ATENÇÃO**: Alunos EJA estão sendo removidos temporariamente até novo tratamento

### 🔧 Correção

#### Commit: `284ccc3` + `d325b91` - 25/09/2026
**fix: corrige cálculo de faltas abonadas considerando abono por dia**

Correção da lógica de abonação de faltas para considerar que o abono é sempre por dia e não por tempo individual

**Arquivos modificados:**
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`

**Mudanças aplicadas:**
1. **Modelo de frequência para COC aberto (freq_coc_atual)**
   - Campo: `numero_faltas`
   - Antes: `sum(falta) - countif(abonaFalta is true and falta > 0)`
   - Depois: `sum(case when abonaFalta is true then 0 else falta end)`

**Impacto:**
- O cálculo de frequência acumulada agora trata corretamente o abono como exclusão de faltas por dia inteiro
- Elimina a contagem de faltas em dias onde há abonação

---

#### Commit: `2e34761` - 24/09/2026
**fix(sme): exclude absences with abonaFalta flag from frequencia acumulada calculation**

Excluir ausências marcadas com flag `abonaFalta` do cálculo de frequência acumulada

**Arquivos modificados:**
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`

**Mudanças aplicadas:**
1. **Modelo de frequência por tempo de aula (tipo_frequencia_apurada = 2)**
   - Campo: `total_falta_tempo`
   - Antes: `sum(falta)`
   - Depois: `sum(case when abonaFalta is true then 0 else falta end)`
   - Descrição: Subtrai a quantidade de faltas justificadas do total de faltas por tempo

2. **Modelo de frequência por dia de aula (tipo_frequencia_apurada = 1)**
   - Campo: `numero_faltas`
   - Antes: `sum(falta)`
   - Depois: `sum(falta) - countif(abonaFalta is true and falta = 1)` (refatorado em commit posterior)
   - Descrição: Subtrai a quantidade de faltas justificadas do total de faltas por dia

**Impacto:**
- Os cálculos de frequência acumulada agora contabilizam corretamente apenas as faltas não justificadas
- Faltas com `abonaFalta = true` não são mais contadas como ausências no cálculo da frequência acumulada
- Melhora na precisão dos relatórios de frequência escolar

### 🧪 Melhoria

#### Commit: `29621e1` - 23/09/2026
**refact: movido o calculo da frequencia para modelo intermediario a fim de garantir o teste antes da materialização final**

Refatoração do cálculo de frequência acumulada para estrutura de modelo intermediário (ephemeral)

**Arquivos modificados:**
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql` (novo)
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.yml` (movido)
- `models/mart/sme/frequencia/mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql` (refatorado)
- `tests/singular/int_frequencia__freq_acumulada_dias_letivos__freq_coc_fechados_igual.sql` (renomeado)
- `tests/singular/int_frequencia__freq_acumulada_dias_letivos__todos_alunos_presentes.sql` (renomeado)

**Mudanças aplicadas:**
1. **Criação do modelo intermediário para testes pré-build**
   - Movido toda lógica de cálculo para `int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`
   - Configurado como modelo ephemeral para execução intermediária + testes
   - Testes movidos para a pasta correta com prefixo `int_`

2. **Refatoração da view principal**
   - View `mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos` agora consome apenas o modelo intermediário

**Impacto:**
- **Os testes agora validam o cálculo intermediário antes da materialização final**

### 🔧 Correção

#### Commit: `3b10990` - 23/09/2026
**fix: corrige o calculo da frequencia para coc em aberto e adiciona teste comparativo de coc aberta e fechado**

Correção da lógica de cálculo de frequência para COC aberto com teste comparativo

**Arquivos modificados:**
- `models/mart/sme/frequencia/mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`
- `models/mart/sme/frequencia/mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos_pivoted.sql`
- `models/raw/sme/educacao_basica/gestao_escolar_vw_bi_avaliacao.sql` (renomeado)
- `models/raw/sme/educacao_basica/gestao_escolar_vw_bi_avaliacao.yml`
- `tests/singular/mart_frequencia__freq_acumulada_dias_letivos__delta_alto_qtd_aulas.sql`
- `tests/singular/mart_frequencia__freq_acumulada_dias_letivos__freq_coc_fechados_igual.sql`
- `tests/singular/mart_frequencia__freq_acumulada_dias_letivos__todos_alunos_presentes.sql`

**Mudanças aplicadas:**
1. **Correção da lógica de frequência por COC aberto**
   - Refatorado CTE `frequencia_coc_atual` com dois modelos distintos:
     - **Tipo 2 (agrupa por dia)**: Calcula frequência agrupando por data de aula
       - Para cada dia, verifica se `total_falta_tempo < total_tempos`
       - Conta como falta apenas se houver ausência em todos os tempos do dia
     - **Tipo 1 (sem agrupar por dia)**: Soma diretamente tempos sem agrupação
   - Consolidação com UNION ALL dos dois tipos de frequência

2. **Aplicação de testes para validação do COC aberto x fechado**
    - **`mart_frequencia__freq_acumulada_dias_letivos__delta_alto_qtd_aulas`**: Valida que não há variação alta demais na quantidade de aulas (ajustado limiar de `abs(taxa_crescimento - 1) > 0.3` para `taxa_crescimento > 1.3`)
    - **`mart_frequencia__freq_acumulada_dias_letivos__freq_coc_fechados_igual`**: Valida se dados dos COCs fechados estão iguais ao o modelo `gestao_escolar_vw_bi_avaliacao`, com tolerência de até 5000 alunos.
    - **`mart_frequencia__freq_acumulada_dias_letivos__todos_alunos_presentes`**: Valida se todos os alunos do modelo `gestao_escolar_vw_bi_avaliacao` estão presentes em `frequencia_acumulada_dias_letivos`, com tolerência de até 5000 alunos.

3. **Limpeza do código**
   - Removido: `ORDER BY id_aluno, ano_calendario desc, id_tipo_calendario desc`
   - Justificativa: Ordenação desnecessária na transformação

**Impacto:**
- **Cálculo frequência para COCs abertos corrigidos em ambos os tipos de apuração**
- Validação com teste comparativo entre COCs abertos e fechados
