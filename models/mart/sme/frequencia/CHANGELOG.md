# Changelog

Todas as mudanças notáveis nos modelos `sme/frequencia` são documentadas neste arquivo.

> O formato utilizado para documentação segue as boas práticas de [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/).

### 🔧 Correção

#### Commit: `3377ec1` - 28/10/2026
**fix: COC fechado/aberto por cne_situacao (cls_confirmacao_efetivacao)**

Corrige o fechamento do COC, que usava a data de fim do COC, e o cálculo do COC aberto, que estava fixo no COC 3 de 2026. Regra validada com a SME: `cne_situacao` 1 ou 5 = COC fechado; os demais (4 ou sem registro) = COC aberto.

**Arquivos modificados:**
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`

**Mudanças aplicadas:**
1. **Situação do COC por calendário** (nova CTE `situacao_coc`)
   - Usa o último registro de `cls_confirmacao_efetivacao` de cada escola, calendário e COC
   - O COC é fechado no calendário se ao menos uma escola o tem com `cne_situacao` 1 ou 5, e a regra vale para todas as escolas (não cruza por `esc_id`)
2. **COC fechado** (CTE `frequencia_cocs_fechados`)
   - Ano corrente: fechado somente se `coc_fechado`. Antes, bastava `cap_datafim < current_date()`
   - Anos anteriores: seguem fechados pela data
3. **COC aberto** (nova CTE `aulas_coc_aberto`)
   - Calculado pelas aulas para qualquer COC do ano corrente ainda não fechado. Antes, só `ano_calendario = 2026 and tpc_id = 3`

**Impacto (dev × prod):**
- `freq_coc_fechados_igual`: 95 (igual a prod)
- `todos_alunos_presentes`: 2.206 (igual a prod)
- `delta_alto_qtd_aulas`: 2.464 (prod: 2.465)
- `id__unique`, `id__not_null` e `frequencia_percentual__between_0_100`: 0
- COC 4 de 2026 passa a existir (548.810 linhas); COC 3 passa de 561.962 para 555.629 linhas

**Pendências:**
- Validar com a SME o critério de fechamento no calendário. Considerando o `esc_id` (status por escola), `freq_coc_fechados_igual` iria a 19.241
- Regra da matrícula ativa não se aplica ao COC aberto (a validar com a SME)
- Anos anteriores seguem fechados pela data (a `vw_alunos_aulas` não tem as aulas completas de 2024)

### 🔧 Correção

#### Commit: `e5f98d8` - 27/10/2026
**fix: uma única aat por aluno/COC nos COCs fechados (regra da matrícula ativa)**

Corrige as divergências SGA × lake apontadas pela CIT/SME, causadas por alunos com matrículas duplicadas.

**Arquivos modificados:**
- `models/mart/sme/frequencia/intermediate/int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos.sql`
- `tests/singular/int__freq_acumulada_dias_letivos__freq_coc_fechados_igual.sql`
- `tests/singular/int_frequencia__freq_acumulada_dias_letivos__todos_alunos_presentes.sql`

**Mudanças aplicadas:**
1. **Uma `aat` por aluno/COC** (CTE `frequencia_cocs_fechados`)
   - Antes: aluno com mais de uma matrícula no calendário gerava mais de uma linha por COC
   - Agora: fica uma única `aat`, escolhida nesta ordem:
     1. a que tem aulas preenchidas;
     2. a de matrícula com situação 1 ou 5 (situação 3 só vale se for a única);
     3. a de matrícula ativa no fim do COC (`cap_datafim`);
     4. se nenhuma estiver ativa, a da última matrícula que saiu antes do fim
2. **Filtro `ava_tipo = 1`:** considera só avaliações de frequência
3. **Testes**
   - `freq_coc_fechados_igual`: o SGA tem uma linha por matrícula, então só há divergência se nenhuma linha bater com o lake. Passa a contar pares, e não linhas
   - `todos_alunos_presentes`: passa a contar pares distintos
4. **Dependência:** `gestao_escolar_vw_bi_avaliacao` entra no upstream do mart (`depends_on`), porque os testes dependem dela

**Impacto (dev × prod):**
- `id__unique`: de 1.020 chaves duplicadas para 0
- `freq_coc_fechados_igual`: de 994 para 780 registros
- `delta_alto_qtd_aulas`: de 2.537 para 2.465 alunos em alerta
- `todos_alunos_presentes`: sem mudança (2.226)

**Pendências:**
- Carga faltante de `aluno_avaliacao_turma` em 31/08 e 30/09 (ingestão)
- COC fechado/aberto por `CLS_ConfirmacaoEfetivacao.cne_situacao` (tabela ainda não ingerida) e remoção do COC 3/2026 fixo no COC aberto
- EJA/PROEJA seguem excluídos temporariamente

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
