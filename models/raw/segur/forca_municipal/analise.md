# Análise de Frequência e Estratégia de Atualização — Pipeline Força Municipal

Análise baseada em dados de produção do dataset `rj-segur.brutos_forca_municipal`,
consultando as tabelas raw diretamente (sem depender dos modelos de monitoramento).

---

## Índice

| Seção | |
|---|---|
| [Contexto operacional](#contexto-operacional) | Objetivo do CompStat Rio e perguntas P1–P4 |
| [Metodologia de coleta](#metodologia-de-coleta) | Métricas coletadas e mecanismo de hashes |
| [Avaliação de endpoints](#avaliação-de-endpoints-o-que-vale-coletar) | Decisões keep/disable; análise de sobreposição; cruzamentos |
| &nbsp;&nbsp;[Resumo por endpoint](#resumo-por-endpoint) | Tabela de decisões para os 12 endpoints |
| &nbsp;&nbsp;[Análise de sobreposição](#análise-de-sobreposição) | Por que cada endpoint foi desativado ou mantido |
| &nbsp;&nbsp;[Cruzamentos e dependências](#cruzamentos-e-dependências-entre-tabelas) | Fontes canônicas P1–P4; cobertura espacial; id_unidade; universos |
| &nbsp;&nbsp;[Pipeline recomendada](#pipeline-recomendada-após-ajustes) | 9 endpoints ativos, 3 desativados |

| Tabela | Tipo | Intervalo atual | Recomendação pipeline | Recomendação dbt |
|---|---|---|---|---|
| [unit_positions](#unit_positions) | Log GPS cumulativo | 5min | 1×/dia D-1 às 03:00 | incremental insert_overwrite, 1×/dia |
| [ocorrencias_ativas](#ocorrencias_ativas) | Snapshot backlog aberto | 5min | **Desativar** | incremental insert_overwrite, 1×/dia |
| [ocorrencias_ativas_v2](#ocorrencias_ativas_v2) | Snapshot despachos ativos | 5min | 5min (manter) | incremental insert_overwrite, 1×/dia |
| [ocorrencias_historico](#ocorrencias_historico) | Histórico full dump | 1×/dia (1 run) | 1×/dia às 03:00 | incremental insert_overwrite, 1×/dia |
| [unidades_ativas](#unidades_ativas) | Snapshot posição/status | 5min | **Desativar** | incremental insert_overwrite, 1×/dia |
| [unidades_historico](#unidades_historico) | Histórico full dump | 1×/dia (2 runs) | 1×/dia às 03:00 | incremental insert_overwrite, 1×/dia |
| [qmd](#qmd) | Catálogo de referência | 1×/dia | 1×/dia | incremental insert_overwrite, 1×/dia |
| [qmd_ativos](#qmd_ativos) | Catálogo filtrado (ativos) | 1×/dia | **Desativar** | incremental insert_overwrite, 1×/dia |
| [qmd_detalhes](#qmd_detalhes) | Catálogo com geometria WKT | 1×/dia | 1×/dia | incremental insert_overwrite, 1×/dia |
| [qmd_kml](#qmd_kml) | Geometrias KML parseadas | 1×/dia | 1×/dia | incremental insert_overwrite, 1×/dia |
| [qmd_plano](#qmd_plano) | Calendário semanal CompStat | 1×/dia | 1×/dia | incremental insert_overwrite, 1×/dia |
| [qmd_servicos](#qmd_servicos) | Alocações viatura ↔ QMD ↔ plano | 1×/dia | 1×/dia | incremental insert_overwrite, 1×/dia |

---

## Contexto operacional

### Objetivo

O painel **CompStat Rio** responde a uma pergunta central:

> **A força municipal está no local certo, na hora certa, com o planejamento certo?**

Para respondê-la, o sistema cruza três planos de dados:

| Plano | O que é | Fonte |
|---|---|---|
| **Crime** | Onde, quando e qual crime ocorreu | SSM microdados, Disk Denúncia |
| **Planejamento** | O que foi planejado: área, horário, viatura | QMD |
| **Execução** | O que a força realmente fez | GPS das viaturas |

### Perguntas operacionais

Dado um recorte (polígono) e um período (semana):

| Pergunta | Dados necessários | Frequência de análise |
|---|---|---|
| P1 — O crime está subindo ou descendo? | SSM microdados | Semanal |
| P2 — O planejamento está no local e horário certo? | QMD (plano, serviços, KML) | Semanal |
| P3 — A força está executando o planejado? | GPS das viaturas vs polígono QMD | Semanal |
| P4 — O planejamento foi executado, mas o crime não caiu. Por quê? | Cruzamento dos três planos | Semanal |

### Implicação para a pipeline

O painel opera em ciclo **semanal**, com análises retrospectivas. A materialização
dbt roda **1× por dia**. O dado do dia anterior precisa estar **completo e limpo**
no início do dia seguinte (D-1). Extrações intradiárias repetidas têm valor apenas
quando necessárias para garantir completude — não por demanda de tempo real.

---

## Metodologia de coleta

Para cada tabela foram coletadas as seguintes métricas diretamente das tabelas raw:

| Métrica | Query | Objetivo |
|---|---|---|
| Partições, runs, volume total | `COUNT DISTINCT data_particao`, `id_hash`, `COUNT(*)` | Entender escala e histórico |
| Registros únicos de negócio | Chave composta sem colunas de pipeline | Medir duplicação real |
| Fator de duplicação | `total_linhas / registros_unicos` | Quantificar impacto do acúmulo |
| Duplicatas intra-partição | `GROUP BY chave + data_particao`, `COUNT > 1` | Detectar acúmulo de runs |
| Duplicatas cross-partição | `COUNT DISTINCT data_particao` por chave | Entender sobreposição entre dias |
| Gap entre runs consecutivos | `LAG(updated_at)` por `id_hash` | Confirmar cadência real |
| Lag entre dado e extração | `updated_at - campo_timestamp_max` por run | Confirmar frescor dos dados |
| Delta entre runs | Ocorrências/registros novos entre runs consecutivos | Medir volatilidade real do dado |
| Mudanças de conteúdo entre runs | Comparar campos entre primeiro e último run | Confirmar se dado muda entre runs |
| Distribuição temporal | `EXTRACT(HOUR FROM campo)` | Entender padrão de atividade |
| Cobertura de campos | `COUNTIF(campo IS NULL)` | Qualidade dos dados |

### Por que há hashes diferentes se os dados são quase iguais?

O `id_hash` é gerado pela função `_content_hash` em `save_partitions_task`:

```python
raw = pd.util.hash_pandas_object(df, index=False).values.tobytes()
return hashlib.md5(raw).hexdigest()[:12]
```

Esse hash é calculado sobre o **DataFrame inteiro** — todas as linhas e colunas
juntas. Se **qualquer coisa mudar** (uma nova linha, um campo atualizado, até a
ordem diferente de uma coluna), o hash muda completamente.

Para tabelas com dados quase estáticos, isso significa que mesmo uma única
ocorrência nova (de 8.775 para 8.776) produz um hash completamente diferente.
Resultado: cada run gera um `id_hash` único, que com `dump_mode: append` vira
um arquivo separado no GCS. A tabela BigQuery (external/staging) enxerga todos
os arquivos e retorna todas as linhas — causando o fator de duplicação observado.

**Resumo:** `n_hashes_distintos = n_runs_que_produziram_conteúdo_diferente`.
Como a API retorna dados dinâmicos (mesmo que levemente diferentes), praticamente
todo run gera um hash novo, e com `append` cada hash novo vira uma cópia adicional
de toda a partição na tabela de destino.

---

## Avaliação de endpoints — o que vale coletar

### Resumo por endpoint

| Endpoint | Dados únicos | CompStat | Pipeline | Decisão |
|---|---|---|---|---|
| `unit_positions` | Rastro GPS contínuo por viatura | P3 aderência ao QMD | 1×/dia D-1 | **Manter** |
| `ocorrencias_ativas` | Backlog de abertas (snapshot) | Tendência semanal do backlog | 1×/hora | **Desativar** |
| `ocorrencias_ativas_v2` | Despachos ativos + lat/lon + agentes | P3 — quem estava despachado, onde | 5min | **Manter** |
| `ocorrencias_historico` | Todas as revisões de todas as ocorrências | P1/P4 — histórico completo de eventos | 1×/dia | **Manter** |
| `unidades_ativas` | Snapshot posição + status a cada 5min | Dashboard real-time | 5min | **Desativar** |
| `unidades_historico` | Transições de status event-driven | P3 — tempo de resposta e disponibilidade | 1×/dia | **Manter** |
| `qmd` | Catálogo completo com `prescricoes` | Lookup de missões | 1×/dia | **Manter** |
| `qmd_ativos` | Subconjunto ativo de `qmd` | — | 1×/dia | **Desativar** |
| `qmd_detalhes` | Geometria WKT + horários + serviços | P2/P3 — onde e quando a viatura deve estar | 1×/dia | **Manter** |
| `qmd_kml` | Boundaries de área por QMD | Visualização geográfica | 1×/dia | **Manter** |
| `qmd_plano` | Calendário semanal CompStat | Âncora temporal de toda análise | 1×/dia | **Manter** |
| `qmd_servicos` | Alocação viatura ↔ QMD ↔ plano | P3 — qual viatura deveria estar onde | 1×/dia | **Manter** |

### Análise de sobreposição

#### `ocorrencias_ativas` → **Desativar**

`ocorrencias_historico` (full dump diário) contém tudo que `ocorrencias_ativas`
tem e mais:

| Dado | `ocorrencias_ativas` | `ocorrencias_historico` |
|---|---|---|
| Backlog de abertas hoje | ✓ | ✓ (filtrar `IsOpen=True` na última revisão) |
| Estado de cada ocorrência na data X | ✓ (partição do dia) | ✓ (partição do dia, mesma lógica) |
| Histórico de status de cada ocorrência | ✗ | ✓ |
| Ocorrências encerradas | ✗ | ✓ |

A pergunta "quantas ocorrências estavam abertas na semana X?" é respondível
diretamente de `ocorrencias_historico`: na partição do dia de referência, filtrar
pela última revisão de cada `AgencyEventId` onde `IsOpen = True`. O snapshot
diário de `ocorrencias_ativas` não agrega nada além de simplicidade de query,
que não justifica o custo de 8.782 linhas × 1×/hora = ~210K linhas/dia na staging.

#### `unidades_ativas` → **Desativar**

Para CompStat (análise semanal retroativa), `unidades_ativas` não tem vantagem
sobre as alternativas já coletadas:

| Dado | `unidades_ativas` | Alternativa |
|---|---|---|
| Posição GPS contínua para aderência | 5min granularidade | `unit_positions` — GPS a cada poucos minutos, por viatura, data e hora |
| Transições de status com timestamp preciso | ✗ (só snapshot 5min) | `unidades_historico` — event-driven, precisão de segundos |
| Status atual para dashboard real-time | ✓ | Sem alternativa direta |

O único dado único de `unidades_ativas` é o estado atual para um dashboard
real-time. Como o CompStat opera em ciclo semanal retroativo e o dbt roda 1×/dia,
esse valor nunca chega ao dashboard a tempo de ser "real-time". Se um painel
ao vivo for necessário no futuro, pode-se reativar o endpoint e consumir a staging
diretamente (sem passar pelo dbt).

#### `qmd_ativos` → **Desativar**

É `qmd` com `WHERE StatusAtivo = True`. Toda query que usa `qmd_ativos` pode
usar `qmd` com o mesmo filtro. A extração duplicada adiciona um run e uma tabela
sem nenhum dado exclusivo.

#### `qmd` — Manter (não redundante com `qmd_detalhes`)

`qmd_detalhes` é chamado por ID (`/api/qmd/{id}`) e retorna um subconjunto de
campos. `qmd` adiciona campos ausentes em `qmd_detalhes`:

| Campo | `qmd` | `qmd_detalhes` |
|---|---|---|
| `Id`, `Nome`, `Area`, vigência, status | ✓ | ✓ |
| `Prescricoes` (instruções táticas) | ✓ | ✗ |
| `IdRespCriacao`, `IdRespAutorizacao` | ✓ | ✗ |
| `DataHoraCriacao`, `DataHoraAutorizacao` | ✓ | ✗ |
| `missoes` (geometria + serviços + execuções) | ✗ | ✓ |

`Prescricoes` tem valor qualitativo (instruções da missão). Os timestamps de
criação são úteis para auditoria e ordenação. Manter `qmd` como catálogo
administrativo; `qmd_detalhes` como fonte geoespacial.

### Cruzamentos e dependências entre tabelas

#### Mapa de fontes canônicas por pergunta

Para cada pergunta do CompStat, existe uma fonte primária e fontes de suporte.
Usar a fonte errada pode produzir resultados corretos na aparência mas com limitações
ocultas — cobertura parcial, resolução inadequada ou universo diferente.

| Pergunta | Fonte primária | Fonte de suporte | Limitação principal |
|---|---|---|---|
| P1 — O crime está subindo? | SSM microdados | `ocorrencias_historico` | `ocorrencias_historico` cobre desde 2026-03-15 (go-live do CAD); crimes anteriores só no SSM |
| P2 — Planejamento no local e horário certo? | `qmd_detalhes` (geometria + horário) | `qmd_plano`, `qmd_servicos`, `qmd_kml` | Tipos DS/SP/SV não documentados — não interpretar em mart sem confirmar com GM |
| P3 — A força executou o planejado? (geográfico) | `unit_positions` (GPS ~23s) | `qmd_detalhes` (polígonos) | Join por `id_unidade` = `qmd_servicos.nome` — formato `TIPO##-BASE` confirmado consistente |
| P3 — A força executou o planejado? (disponibilidade) | `unidades_historico` (event-driven) | `qmd_servicos`, `qmd_plano` | 11 status codes; UE, AM, UG sem significado documentado |
| P4 — Planejamento executado, crime não caiu. Por quê? | Cruzamento dos três planos | Todos acima | Requer join multi-tabela; alinhar por semana de `qmd_plano` |
| Backlog de abertas por semana | `ocorrencias_ativas` (snapshot diário) | `ocorrencias_historico` (reconstrução) | Snapshot direto é mais simples; reconstrução de `ocorrencias_historico` é possível mas custosa |
| Despachos ativos e presença de agentes | `ocorrencias_ativas_v2` | `unidades_historico` (status DP+QE) | v2 cobre < 5% das ocorrências (apenas com despacho ativo no momento do polling) |

#### Cobertura espacial de ocorrências: < 5%

`ocorrencias_ativas_v2` é o único endpoint com coordenadas (`latitude`/`longitude`)
de ocorrências. No entanto, a cobertura é muito menor do que o nome sugere:

- Em ~2h de observação: **8 ocorrências únicas com coordenada** de um universo
  de 8.782 ativas e ~165 novas por dia
- O endpoint retorna apenas ocorrências com **despacho ativo no momento do polling**
  — não todas as abertas
- Uma ocorrência que nunca recebe despacho (ficou aberta sem unidade atribuída)
  **nunca aparece em v2**
- Estimativa de cobertura: **< 5% das ocorrências têm coordenada** no dataset

Implicações práticas:

- Análises de localização de ocorrência em geral precisam do SSM microdados
  (que tem endereço geocodificado), não de `ocorrencias_ativas_v2`
- `ocorrencias_ativas_v2` tem valor específico para P3 ("agente estava em cena
  na área correta?"), não para análise geográfica do universo de ocorrências
- O cruzamento `ocorrencias_ativas_v2` × `qmd_detalhes` (point-in-polygon) é
  válido mas representa uma fração pequena do universo total

#### `id_unidade` é consistente entre todas as tabelas

O `id_unidade` é a chave que liga execução (GPS, histórico) ao planejamento (serviços).
Verificado em produção — o formato é idêntico nas três tabelas relevantes:

| Tabela | Coluna | Exemplos verificados |
|---|---|---|
| `unit_positions` | `id_unidade` | `POG01-LITORANEA`, `MOT01-NORTE`, `POG01-OESTE` |
| `unidades_historico` | `id_unidade` | `POG01-LITORANEA`, `MOT01-NORTE`, `POG01-OESTE` |
| `qmd_servicos` | `nome` | `POG01-LITORANEA`, `MOT01-NORTE`, `POG01-OESTE` |

Formato uniforme `TIPO##-BASE` em todos os endpoints. O join P3 central funciona
diretamente, sem normalização:

```sql
qmd_servicos.nome = unit_positions.id_unidade
qmd_servicos.nome = unidades_historico.id_unidade
```

#### Universos de unidades divergentes

O "universo da frota" não é consistente entre tabelas:

| Tabela | Unidades | Observação |
|---|---|---|
| `unidades_ativas` | 71 | Logadas hoje |
| `unit_positions` | 56 | GPS observado (coleta iniciou às 09h — frota parcial) |
| `unidades_historico` | 79 | Históricas em 63 dias de operação |
| `qmd_servicos` | 76 | Com alocação no planejamento |

Consequências práticas:

- **71 ativas vs 76 alocadas:** 5 unidades estão no planejamento mas não estavam
  logadas. Em análise de aderência, teriam cobertura 0% — ausência real de execução,
  não ausência de dado
- **79 históricas vs 76 alocadas:** 3 unidades aparecem no histórico mas nunca
  receberam alocação em `qmd_servicos`. Podem ser unidades de supervisão ou de
  um período anterior ao planejamento QMD
- **56 em `unit_positions` (observação parcial):** coleta de hoje não cobre o dia
  completo. Em extração D-1 (03:00, dia fechado), o número esperado é próximo de
  71–79

Para joins com cobertura total, usar `unidades_historico` como universo de referência
(79 unidades, ~63 dias de operação) com `LEFT JOIN` para `qmd_servicos` — unidades
sem alocação representam patrulhamento fora do planejamento QMD.

### Pipeline recomendada após ajustes

```
Manter ativos (8 de 12 endpoints):
  unit_positions          — 1×/dia D-1
  ocorrencias_ativas_v2   — 5min
  ocorrencias_historico   — 1×/dia
  unidades_historico      — 1×/dia
  qmd                     — 1×/dia
  qmd_detalhes            — 1×/dia
  qmd_kml                 — 1×/dia
  qmd_plano               — 1×/dia
  qmd_servicos            — 1×/dia

Desativar (3 de 12 endpoints):
  ocorrencias_ativas      — redundante com ocorrencias_historico
  unidades_ativas         — redundante com unit_positions + unidades_historico
  qmd_ativos              — redundante com qmd (filtro StatusAtivo)
```

---

## unit_positions

### Dados coletados

**Visão geral da tabela (snapshot atual):**

| Métrica | Valor |
|---|---|
| Partições existentes | 1 (2026-05-16) |
| Runs distintos (`id_hash`) | 26 |
| Janela de runs | 09:07 → 11:16 |
| Total de linhas na tabela | 223.899 |
| Registros únicos de negócio | 13.138 |
| Fator de duplicação | 17,0× |
| Linhas duplicadas intra-partição | 223.409 (99,8%) |
| Máximo de cópias do mesmo registro | 26 (= nº de runs) |
| Nulls em campos críticos | 0 |
| Unidades distintas | 56 |
| Janela de dados GPS | 00:00:07 → 11:14:35 |

**Por que 26 hashes diferentes se o dado parece repetido?**

O endpoint `/api/unit/positions` com `hora_inicio=00:00:00` retorna todas as
posições do dia desde meia-noite. A cada run de 5 min, o dataset cresce com novas
posições — run de 09:07 tem 4.163 registros únicos, run de 11:16 tem 13.042. Como
o conteúdo muda a cada run (mais posições), o MD5 do DataFrame inteiro muda, gerando
um `id_hash` diferente. Com `dump_mode: append`, cada um dos 26 arquivos coexiste
na partição GCS e a staging table retorna todos — resultando em 223.899 linhas onde
existem apenas 13.138 posições únicas (fator 17×).

**Cadência real dos runs:**

| Métrica | Valor |
|---|---|
| Gap mínimo | 269s (4,5 min) |
| Gap médio | 310s (5,2 min) |
| Gap mediano | 303s (5,0 min) |
| Gap máximo | 466s (7,8 min) |

Alinhado com `interval: 300`. O run demora ~4 min (busca paralela de 56 unidades),
deixando ~1 min de margem até o próximo start.

**Natureza do dado — lag entre posição e extração:**

| Run | Extração (`updated_at`) | Posição mais recente | Lag |
|---|---|---|---|
| 09:07 | 09:07:54 | 09:07:46 | **8s** |
| 09:15 | 09:15:40 | 09:15:25 | 15s |
| 09:21–11:16 | — | — | ~140s estável |

O lag de ~140s nos runs seguintes é o tempo de coleta das 56 unidades dividido por 2
(defasagem média entre a primeira e a última unidade coletada no run). O dado é
**em tempo real**: GPS de cada viatura envia posições continuamente, a API as serve
com ~2 min de defasagem técnica de coleta.

**Crescimento acumulado de posições únicas por run:**

| Hora | Posições únicas | Novas posições |
|---|---|---|
| 09:07 | 4.163 | — (início) |
| 09:35 | 6.442 | +2.279 (turno entrando) |
| 10:11 | 8.372 | +1.930 |
| 11:16 | 13.042 | +4.670 vs 09:07 |

Crescimento de ~69 posições únicas/minuto no pico operacional.

**Distribuição de intervalos entre posições por unidade:**

| Faixa | % |
|---|---|
| ≤ 10s | 22,2% |
| 11–30s | 60,4% |
| 31–60s | 10,0% |
| 1–5 min | 6,7% |
| > 5 min | 0,7% |

Ticker natural dos GPS: **~23s** (mediana). 82,6% das posições chegam a cada ≤ 30s.

**Cobertura por hora:**

| Hora | Unidades ativas | Posições |
|---|---|---|
| 00–07h | 5–6 | ~4.676 |
| 08h | 16 | 1.013 |
| 09h | 28 | 3.201 |
| 10h | 48 | 3.866 |

Madrugada: 5–6 unidades de patrulha noturna. Entrada de turno visível às 8–9h.

### Análise

**Dado em tempo real com acumulação diária.** O endpoint retorna todas as posições
desde meia-noite — funciona como um log cumulativo que cresce ao longo do dia.
O dado é genuinamente em tempo real (lag de ~8s para a posição mais recente), mas a
API não oferece parâmetro "buscar apenas novos desde o último run".

**O endpoint tem parâmetro de data (`data_inicio`, `data_fim`).** É possível buscar
o dia anterior passando `data_inicio=D-1` e `data_fim=D-1`. Isso garante completude
total: o dia D-1 já está fechado quando a extração ocorre às 03:00 de D, então todas
as posições de D-1 são retornadas em um único run sem risco de perda ou duplicata
interna. Isso resolve tanto o problema de duplicação quanto o de completude D-1.

**Para o CompStat (P3 — aderência ao planejado):** a análise compara trajetórias GPS
com polígonos QMD em escala diária/semanal. O dado do dia anterior completo é
suficiente — não há necessidade de atualizações intradiárias.

**Risco de rodar 1×/dia para D-1:** nenhum. O endpoint aceita `data_inicio`/`data_fim`
explícitos, então buscar D-1 às 03:00 entrega o dia completo e fechado.

### Recomendações

**Pipeline — Prefect:**
- **Mudar para 1× por dia, às 03:00 AM**, passando `data_inicio=D-1` e
  `data_fim=D-1` como parâmetros. Elimina os 25 runs redundantes, entrega o dia
  anterior completo em um único arquivo por partição, sem acumulação.
- Com um único run por partição, o `dump_mode: append` natural não causa duplicatas —
  1 run = 1 arquivo = 1 partição limpa.
- Monitorar o tempo de execução: à medida que o número de unidades cresce, o run
  pode ultrapassar a janela disponível (atual: ~4 min para 56 unidades).

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Com 1 run/dia
  entregando D-1 completo, o dbt substitui a partição com dado limpo.
- **Rodar dbt logo após a extração** (ex: 04:00 AM).
- A análise P3 (viatura dentro do polígono QMD no horário correto) usa
  `ST_CONTAINS(geometry_qmd, geometry_viatura)` com granularidade de minutos —
  o ticker de 23s por GPS é mais que suficiente para essa análise em nível diário.

---

## ocorrencias_ativas

### Dados coletados

**Visão geral da tabela (snapshot atual):**

| Métrica | Valor |
|---|---|
| Partições existentes | 1 (2026-05-16) |
| Runs distintos (`id_hash`) | 13 |
| Janela de runs | 09:03 → 11:13 |
| Total de linhas na tabela | 114.116 |
| Ocorrências únicas (`id_ocorrencia`) | 8.782 |
| Fator de duplicação | 13,0× |
| Linhas duplicadas intra-partição | 105.334 (92,3%) |
| Máximo de cópias por ocorrência | 13 (= nº de runs) |
| Média de cópias por ocorrência | 12,99 |

**Por que 13 hashes diferentes se o dado muda mínimamente?**

O endpoint `/api/ocorrencias/ativas` não tem parâmetro de data — retorna o estado
corrente de todas as ocorrências abertas. A cada run de 5 min, 1–2 novas ocorrências
entram na lista (ou o campo `data_hora_atualizacao_bd` de alguma muda). Isso altera
o DataFrame inteiro, muda o MD5, gera um `id_hash` novo. Com `dump_mode: append`,
cada run adiciona um arquivo com todas as ~8.780 linhas novamente na partição GCS.
Resultado: 13 arquivos × ~8.780 linhas = 114.116 linhas, sendo apenas 8.782 únicas.

**Linhas e ocorrências únicas por run:**

| Hora | Ocorrências únicas | Delta vs anterior |
|---|---|---|
| 09:03 | 8.775 | — (início) |
| 09:33 | 8.776 | +1 |
| 09:43 | 8.776 | 0 |
| 09:48 | 8.776 | 0 |
| 09:58 | 8.777 | +1 |
| 10:08 | 8.778 | +1 |
| 10:48 | 8.780 | +2 |
| 11:08 | 8.781 | +1 |
| 11:13 | 8.782 | +1 |

Em 2h10min, apenas **+7 novas ocorrências** apareceram (~3/hora nesse período).

**Mudanças de conteúdo entre primeiro e último run:**

| Métrica | Valor |
|---|---|
| Ocorrências presentes em ambos os runs | 8.775 |
| Mudaram de `id_status` | **0** |
| Mudaram `data_hora_ultima_mudanca_status` | **0** |

As 8.775 ocorrências comuns são **byte-a-byte idênticas** entre o 1º e o último run.
O hash muda exclusivamente pela adição de novas linhas, não por mudança de conteúdo.

**Gap entre runs:**

| Métrica | Valor |
|---|---|
| Gap mínimo | 298s (≈ 5 min) |
| Gap mediano | 301s (≈ 5 min) |
| Gap máximo | 2.101s (35 min — janela sem runs entre 10:13–10:48) |

**Lag entre dado e extração:**

| Momento | Lag `updated_at` → `data_hora_atualizacao_bd` |
|---|---|
| 1º run (09:03) | 1.503s (25 min — warm-up) |
| Runs subsequentes | 134–340s (2–6 min) |

**Características do snapshot (último run):**

| Métrica | Valor |
|---|---|
| Total de ocorrências ativas | 8.782 |
| Tipos de ocorrência distintos | 8 |
| Status distintos | 3 |
| Áreas distintas | 8 |
| Ocorrência mais antiga (abertura) | 2026-03-15 (62 dias atrás) |
| Ocorrência mais recente (abertura) | 2026-05-16T11:09 |
| Idade mediana das ocorrências | **24 dias** |
| Idade p90 | 48 dias |
| Mudaram status nos últimos 5min | 0 |
| Mudaram status na última hora | 2 |
| Mudaram status hoje | 253 |
| Abertas hoje | 252 |

### Análise

**O endpoint não tem parâmetro de data — retorna apenas o estado atual.** Não é
possível buscar "quais ocorrências estavam ativas ontem às 23:59". O que a API
entrega é um snapshot instantâneo da fila de abertas no momento da chamada. Isso tem
uma consequência crítica para completude: **ocorrências que abriram e fecharam no
mesmo dia não aparecem nesse endpoint** — elas só existem em `ocorrencias_historico`.

**Implicação para a garantia de D-1 completo:** rodar `ocorrencias_ativas`
1×/dia não garante que nenhuma ocorrência foi perdida. Uma ocorrência aberta às 10:00
e encerrada às 18:00 nunca apareceria no snapshot das 23:00. Para ter o histórico
completo de D-1, a fonte correta é `ocorrencias_historico`, não `ocorrencias_ativas`.

**O que `ocorrencias_ativas` representa então?** É a fila de trabalho em aberto —
ocorrências que ainda não foram encerradas. Como a mediana de idade é 24 dias, a
grande maioria das ativas é "crônica" (abertas há semanas). O snapshot diário capta
esse estado com fidelidade. Para o CompStat, é útil para responder "qual é o
acúmulo de ocorrências não encerradas por área?" — uma métrica de backlog operacional.

**A frequência de 5 min extrai 13 cópias quase idênticas.** Em 2h10min:
zero mudanças de status nas existentes, apenas +7 novas entradas. O custo de 13
runs traz retorno mínimo: capturar novas ocorrências 5 min mais cedo vs. 2h mais
tarde não muda o resultado analítico semanal do CompStat.

**Por que não reduzir para 1×/dia?** Porque não há garantia de que uma ocorrência
não passe pelo estado "ativa" em menos de 1 dia. Embora a mediana seja 24 dias,
pode haver ocorrências com ciclo de vida de horas. Reduzir muito a frequência aumenta
o risco de perder ocorrências de curta duração nesse endpoint. A frequência correta
é um balanço entre esse risco e o custo de extrações redundantes.

**Dado que `ocorrencias_historico` cobre o histórico completo**, o papel de
`ocorrencias_ativas` no CompStat é complementar: mostra o backlog atual, não o
histórico de eventos. Para as perguntas P1–P4, `ocorrencias_historico` é a fonte
primária de verdade.

### Recomendações

**Pipeline — Prefect:**
- **Reduzir de 5min para 1×/hora.** A volatilidade real é baixíssima (~3 novas
  ocorrências/hora, zero mudanças de status entre runs consecutivos). Rodar a cada
  hora captura ocorrências com ciclo de vida ≥ 1h, que cobre virtualmente todos os
  casos relevantes dado que a mediana de duração é 24 dias.
- Para ocorrências de ciclo muito curto (< 1h), a fonte correta é
  `ocorrencias_historico`, não este endpoint.
- Manter `dump_mode: append`. Com 24 runs/dia (1/hora) em vez de 288 (1/5min),
  o fator de duplicação cai de ~288× para ~24×, reduzindo drasticamente o volume
  de dados acumulados na staging.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Pelo mesmo
  raciocínio de `qmd`: o snapshot diário permite responder "quantas ocorrências
  estavam abertas na segunda-feira da semana X?" diretamente, sem precisar
  reconstruir a partir de `ocorrencias_historico`. Cada partição = estado do
  backlog de abertas naquele dia — essencial para análise de tendência semanal
  no CompStat.
- `ocorrencias_historico` não substitui esse dado: reconstruir o conjunto de
  abertas em uma data específica a partir do histórico de revisões é possível
  mas custoso. O snapshot diário de `ocorrencias_ativas` entrega isso de graça.
- Como a staging acumula múltiplas cópias por dia (fator 13×), o filtro
  incremental deve usar o `MAX(updated_at)` da partição mais recente para garantir
  que o dbt leia o snapshot mais completo do dia:
  ```sql
  WHERE data_particao >= (SELECT MAX(data_particao) FROM {{ this }})
  ```
- Rodar dbt **1× por dia**, ao final do dia (ex: 23:50 BRT) para capturar o
  snapshot mais atualizado antes da virada de partição.

---

## ocorrencias_ativas_v2

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Partições existentes | 1 (2026-05-16) |
| Runs distintos (`id_hash`) | 9 |
| Janela de runs | 09:05 → 11:14 |
| Total de linhas na tabela | 19 |
| Ocorrências únicas (`id_ocorrencia`) | 8 |
| Fator de duplicação | 2,4× |
| Linhas duplicadas intra-partição | 11 (57,9%) |
| Máximo de cópias por ocorrência | 9 (= nº de runs) |
| Mínimo de cópias por ocorrência | 1 |

**Linhas e ocorrências únicas por run:**

| Hora | Linhas | Ocorrências únicas | Gap vs anterior |
|---|---|---|---|
| 09:05 | 1 | 1 | — |
| 09:34 | 2 | 2 | 1.709s (28 min) |
| 09:44 | 2 | 2 | 600s (10 min) |
| 09:59 | 2 | 2 | 900s (15 min) |
| 10:09 | 2 | 2 | 601s (10 min) |
| 10:49 | 3 | 3 | 2.400s (40 min) |
| 10:54 | 2 | 2 | 299s (5 min) |
| 11:09 | 2 | 2 | 900s (15 min) |
| 11:14 | 3 | 3 | 300s (5 min) |

Os gaps entre runs são muito irregulares (5 min a 40 min) apesar do `interval: 300`.
Como cada run extrai pouquíssimos registros (1–3 linhas), a execução é quase
instantânea — o Prefect pode estar agrupando ou atrasando runs por outros motivos
(worker occupado, throttling). Os gaps grandes (28 min, 40 min) indicam que alguns
runs foram perdidos ou atrasados.

**Lag entre dado e extração:**

| Hora | Lag (`updated_at` → `data_hora_atualizacao_bd`) |
|---|---|
| 09:05 (1º run) | 1.602s (27 min — warm-up) |
| Runs seguintes | 141–398s (2–7 min) |

**Conteúdo do último snapshot (11:14):**

| `id_ocorrencia` | Tipo | Status | Área | Abertura | Agentes |
|---|---|---|---|---|---|
| FORCA2026010206199 | OT — Outros | 8 | AP1.1 | 2026-04-30 | 2 agentes nomeados |
| FORCA2026010209328 | OT — Outros | 8 | AP5.2 | 2026-05-16T11:05 | NULL |
| FORCA2026010209329 | A — Abordagem | 8 | AP1.1 | 2026-05-16T11:09 | NULL |

**Distribuição de cópias por ocorrência:**

| Ocorrência | Aparece em quantos runs |
|---|---|
| FORCA2026010206199 (aberta em abril) | 9 de 9 — presente em **todos** os runs |
| FORCA2026010209328 (aberta 11:05) | 2 (entrou no run das 10:49 e 11:14) |
| FORCA2026010209329 (aberta 11:09) | 1 (só no último run) |
| Demais 5 ocorrências | 1–3 cada — passaram pela fila e saíram |

### Análise

**Este endpoint retorna despachos ativos, não toda a fila aberta.** A v1 tem
8.782 ocorrências — a v2 tem 1–3 em cada snapshot. A diferença é o filtro: a v2
parece retornar apenas ocorrências com despacho ativo (unidades atribuídas e a
caminho ou em cena), confirmado pela presença do campo `agentes` (lista de
funcionários designados) e pelas coordenadas GPS de localização da ocorrência.

**A lógica de duplicação aqui é diferente das outras tabelas.** Com `dump_mode:
append` e conteúdo genuinamente diferente entre runs (cada run captura um estado
diferente de quais despachos estão ativos), cada `id_hash` representa um snapshot
real e distinto do sistema. A ocorrência `FORCA2026010206199` aparece em todos os
9 runs porque esteve ativamente despachada por pelo menos 2h seguidas. As outras
passaram pelo endpoint por períodos curtos (1–3 snapshots).

**As linhas acumuladas não são desperdício — são uma série temporal de despachos.**
Ao contrário de `ocorrencias_ativas` (onde 13 cópias são redundantes), aqui as 19
linhas carregam informação temporal: `updated_at` de cada linha diz *quando* aquela
ocorrência foi vista no estado de despacho ativo. Isso permite calcular duração
de despacho e janelas de presença por área — dado direto para P3.

**O fator de duplicação baixo (2,4×) confirma isso:** a maioria das ocorrências
aparece poucas vezes (ciclos curtos de despacho), e apenas as de longa duração
acumulam cópias. Não é um problema de acúmulo descontrolado como nas outras tabelas.

**Gaps irregulares entre runs são preocupantes.** Com 40 min sem extração em
determinado momento, um despacho que abriu e fechou nessa janela seria completamente
perdido. Para uma tabela cujo valor está exatamente em capturar todos os despachos
do dia, gaps grandes são uma lacuna real de cobertura.

**Relação com o CompStat:** esta tabela é a mais diretamente útil para P3
("a força executou o planejado?"). Cruzando `id_ocorrencia`, `agentes`, `latitude`,
`longitude` e `updated_at` com os polígonos do QMD e os horários planejados, é
possível responder se as unidades estavam nas áreas certas no momento certo.
O campo `agentes` (JSON com `EmployeeId` e nome) permite ligar despachos a unidades
físicas — informação ausente na v1.

### Recomendações

**Pipeline — Prefect:**
- **Manter 5min**, mas investigar e corrigir os gaps irregulares (28 min, 40 min
  observados). Com runs de execução quase instantânea (1–3 linhas), o problema está
  no scheduler ou no worker pool. Garantir que o schedule de `ocorrencias_ativas_v2`
  tenha slot dedicado e não concorra com runs mais pesados.
- A frequência de 5 min é justificada aqui — ao contrário de `ocorrencias_ativas`,
  o conteúdo muda genuinamente entre runs e cada snapshot captura despachos que
  podem durar menos de 10 min. Reduzir a frequência criaria lacunas de cobertura.
- `dump_mode: append` é correto para esta tabela: cada run adiciona um snapshot
  real e distinto da fila de despachos ativa.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** As linhas
  acumuladas ao longo do dia formam a série temporal de despachos — cada linha é
  um evento observado em `updated_at`. Ao final do dia, a partição contém todos
  os estados capturados, permitindo reconstruir quais despachos estavam ativos em
  cada janela de 5 min.
- **Não deduplicar por `id_ocorrencia`** no modelo dbt — isso destruiria a série
  temporal. A chave de análise é (`id_ocorrencia`, `updated_at`): a mesma ocorrência
  em dois `updated_at` diferentes são dois eventos distintos.
- Modelos downstream (mart) devem agregar por `id_ocorrencia` + `data_particao`
  usando `MIN(updated_at)` e `MAX(updated_at)` para derivar início e fim do despacho,
  e `ARRAY_AGG(agentes)` para consolidar os agentes envolvidos.
- Rodar dbt **1× por dia**, ao final do dia ou início do seguinte, para que a
  partição do dia reflita todos os snapshots acumulados.

---

## ocorrencias_historico

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 92.738 |
| Runs distintos (`id_hash`) | 1 (pipeline iniciado hoje) |
| Ocorrências únicas (`AgencyEventId`) | 7.927 |
| Média de revisões por ocorrência | 11,7 |
| Revisões — mediana / p75 / máximo | 7 / 19 / 386 |
| Partições | 1 (2026-05-16) |
| Período de eventos coberto | 2026-03-15 → 2026-05-16 |

**Parâmetros do endpoint:** `GET /api/ocorrencias/historico`
— apenas `page` e `pageSize`. **Sem filtro de data.** Cada chamada retorna toda
a história disponível no sistema.

**Novas ocorrências abertas por dia (`RevisionNumber = 1`):**

| Período | Média diária |
|---|---|
| 2026-03-15 → 2026-03-31 | ~60/dia |
| 2026-04-01 → 2026-04-15 | ~130/dia |
| 2026-04-16 → 2026-04-30 | ~155/dia |
| 2026-05-01 → 2026-05-15 | ~165/dia |

Volume crescendo 3× em 2 meses. Total de 7.927 ocorrências em ~62 dias = média
de 128/dia no período.

**Distribuição de status (`StatusCode`):**

| StatusCode | Linhas | Ocorrências únicas |
|---|---|---|
| 7 | 72.646 (78%) | 7.927 (todos — é o status inicial) |
| 8 | 12.944 (14%) | 3.130 |
| 16 | 6.544 (7%) | 3.124 |
| 22 | 545 (0,6%) | 240 |
| 12 | 59 (0,1%) | 59 |

**Estado de abertura:**

- 7.927 ocorrências têm ao menos uma revisão com `IsOpen = True`
- 3.183 ocorrências têm ao menos uma revisão com `IsOpen = False` → foram encerradas
- ~4.744 ocorrências nunca tiveram revisão com `IsOpen = False` → ainda abertas

**Outliers de revisão:**

5 ocorrências acumulam 241–386 revisões, TODAS dentro de uma janela de 5–7 horas
na data de abertura (ex: `FORCA2026010200309`, aberta em 2026-03-16 às 19:35,
última revisão às 01:59 do dia seguinte = 386 revisões em 6,5h = ~1 revisão/min).
Nenhuma foi encerrada. Provável anomalia do sistema CAD (polling automático muito
frequente para essas ocorrências). Não afetam os dados pois representam < 0,1%
das ocorrências.

### Análise

**O endpoint não tem parâmetro de data — cada run extrai toda a história.**
Diferente de `unit_positions` (que aceita `dataInicio`/`dataFim`), este endpoint
retorna TODAS as revisões de TODAS as ocorrências em cada chamada. Hoje, isso são
92.738 linhas; em 6 meses, estimado em ~500K linhas por run.

**Modelo de particionamento da staging:** `data_particao = DATE(updated_at)` =
data do run. Todos os 92.738 registros têm `data_particao = 2026-05-16`. Amanhã,
um novo run gera ~93.500 registros com `data_particao = 2026-05-17`, e assim por
diante.

**Destino dbt como série de snapshots diários:** com `incremental +
insert_overwrite by data_particao`, cada partição na tabela destino contém um
snapshot COMPLETO do estado do sistema naquele dia. A partição de 2026-05-16 tem
todas as 7.927 ocorrências com todos os status; a de 2026-05-17 terá as mesmas
mais as novas criadas no dia. Isso permite consultas point-in-time ("como estavam
as ocorrências em D-3?") mas duplica o volume a cada run.

**Crescimento projetado:**
- Hoje: 92K linhas / partição
- Em 6 meses: ~500K linhas / partição, ~90M linhas totais na tabela destino

Ainda gerenciável no BigQuery (custo de armazenamento < R$ 200/ano), mas confirma
que rodar mais de 1×/dia seria desperdício puro — cada run extra adiciona um
snapshot idêntico ao anterior com ~1h de diferença.

**Para CompStat, o dado útil está em `DatabaseInsertTime` e `RevisionTime`:**
a data de criação da ocorrência no sistema CAD (`CreatedTime`) e o timestamp de
cada revisão (`RevisionTime`) ficam dentro dos campos, não na partição. Para
responder "quantas ocorrências foram abertas ontem?", filtra-se por
`DATE(CreatedTime) = CURRENT_DATE - 1` na última partição.

**O endpoint cobre desde o início do sistema** (2026-03-15 = provável data de
go-live do HxGN CAD). Ocorrências anteriores a essa data não existem neste
endpoint.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia às 03:00 BRT**, após a virada da data. Garante que o snapshot do dia
  D-1 inclua ocorrências encerradas até meia-noite. A janela noturna tem volume
  baixo de novos eventos — risco de perder dados frescos é mínimo.
- `dump_mode: append` é correto (como todas as tabelas desta pipeline). Cada run
  diário cria uma nova partição na staging.
- **Não aumentar frequência.** O endpoint retorna a história completa (92K+ linhas)
  independente da frequência. Rodar 2×/dia duplica o custo de extração e
  armazenamento sem agregar informação nova — a diferença entre dois runs do mesmo
  dia é de horas, não relevante para análise CompStat semanal.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Cada partição
  = snapshot do dia. Correto e eficiente para o padrão de uso do CompStat.
- Modelos downstream (mart) devem sempre ler a **última partição** para obter o
  estado atual:
  ```sql
  where data_particao = (select max(data_particao) from {{ ref('raw_segur_forca_municipal__ocorrencias_historico') }})
  ```
- Para análise de estado de uma ocorrência ao longo do tempo, ordenar por
  `numero_revisao` dentro de cada `id_ocorrencia`.
- Filtro de outliers: considerar `WHERE numero_revisao <= 50` ou similar para
  excluir as anomalias de 386 revisões em análises que usam contagem de revisões
  como proxy de complexidade.
- Rodar dbt **1×/dia** logo após a extração da pipeline (ex: 04:00 BRT).

---

## unidades_ativas

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 5.186 |
| Runs distintos (`id_hash`) | 97 |
| Janela de runs | 08:54 → 16:37 UTC (~7h43min) |
| Intervalo entre runs | exatamente 5min (sem gaps) |
| Unidades únicas (`UnitId`) | 71 |
| Frota por tipo | POG: 39 · VTR: 22 · MOT: 10 |
| Registros com GPS ativo | 100% |
| Partições | 1 (2026-05-16) |

**Unidades por run ao longo do dia:**

| Hora (UTC) | Unidades logadas |
|---|---|
| 08:54 | 16 |
| 09:32 | 24 |
| 10:12 | 30 |
| 10:47 | 40 |
| 11:02 | 51 |
| 11:12 | 56 |
| 12:02 | 60 |
| 13:02 | 64 |
| 15:17 | 71 (máximo) |
| 16:37 | 71 (estável) |

**Status das unidades (todas as linhas):**

| Status | Linhas | % | Unidades |
|---|---|---|---|
| 4 (disponível) | 4.404 | 84,9% | 70 |
| 12 (indisponível) | 434 | 8,4% | 49 |
| 7 (despachada) | 348 | 6,7% | 19 |

**Padrão de entrada na frota:**
- Unidades NORTE e OESTE: presentes desde o 1º run (08:54 UTC = 05:54 BRT)
- Unidades LITORANEA: maioria entra entre 12:02–15:17 UTC (09:02–12:17 BRT)
- MOT (motos): todas entram entre 12:47–13:07 UTC (09:47–10:07 BRT)

**Lag entre evento e extração:**

`StatusChangeTime` armazenado sem timezone (possível BRT no campo). Aplicando
correção de UTC-3: a mediana do lag real entre última mudança de status e extração
é de aproximadamente 4–6 minutos, compatível com o intervalo de 5min do polling.

### Análise

**O polling de 5min funciona perfeitamente nesta tabela.** 97 runs sem nenhum gap,
intervalo consistente, cobertura total de GPS. Esta é a tabela com melhor saúde
operacional da pipeline.

**A frota não está completa ao amanhecer — cresce ao longo do turno.** Às 05:54
BRT (primeiro run), apenas 16 viaturas estavam logadas. A frota só atinge as 71
unidades por volta de 12:00 BRT. Isso é um padrão operacional normal de troca e
início de turno, mas significa que análises de disponibilidade matutina devem
considerar esse ramp-up. Em especial, as unidades LITORANEA entram tardiamente
(a partir de 09:00 BRT).

**Fator de acumulação: 73× por design.** 5.186 linhas / 71 unidades = 73 cópias
por unidade em média, que corresponde exatamente ao número de runs (97). Ao
contrário de outras tabelas onde o acúmulo é efeito colateral do mecanismo de
hash, aqui cada linha é um ponto distinto na série temporal de estados da frota.
Unidades com apenas 1 status distinto no dia têm linhas de conteúdo idêntico, mas
com `updated_at` diferentes — o que é correto: confirma que a unidade permaneceu
no mesmo estado por todo o período.

**Status 12 (indisponível) afeta 49 das 71 unidades em algum momento.** Apenas
22 unidades ficaram exclusivamente em status 4 (disponível) durante toda a
observação. Isso indica que indisponibilidade temporária é rotineira — não anomalia.

**Relação com `unidades_historico`:** enquanto `unidades_ativas` é snapshot
periódico (5min), `unidades_historico` é event-driven (uma linha por transição de
status). Para análise de disponibilidade com precisão de segundos, use
`unidades_historico`. Para o mapa em tempo real e séries temporais de 5min,
use `unidades_ativas`.

### Recomendações

**Pipeline — Prefect:**
- **Manter 5min.** O dado é genuinamente volátil (posição GPS, status e evento
  atribuído mudam continuamente). O polling está funcionando perfeitamente — sem
  gaps, sem atrasos.
- Nenhuma alteração recomendada para esta tabela.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** As ~5.000
  linhas diárias formam a série temporal de estados da frota ao longo do dia.
  Cada linha (`id_unidade` + `updated_at`) é um evento distinto e válido.
- Modelos downstream (mart) devem:
  - Para **estado atual**: filtrar por `updated_at = MAX(updated_at)` na última
    partição
  - Para **análise de disponibilidade**: agrupar por `id_unidade` + janelas de 5min,
    calcular `% tempo por status`
  - Para **cobertura geográfica**: usar `latitude`/`longitude` de cada snapshot
    com `ST_GEOGPOINT` para criar heatmaps de presença por área
- Rodar dbt **1×/dia**, consolidando todas as ~5.000 linhas do dia anterior.
- Filtrar `Latitude = 0 AND Longitude = 0` no mart (0% hoje, mas pode ocorrer no
  futuro conforme documentação da API).

---

## unidades_historico

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 150.659 |
| Runs distintos (`id_hash`) | 2 |
| Janela de runs | 09:03 → 09:12 UTC (9 min de diferença) |
| Eventos únicos (`ActiveUnitHistoryId`) | 75.334 |
| Fator de duplicação | 2,0× (exato — 1 cópia por run) |
| Linhas duplicadas | 75.325 |
| Unidades únicas (`UnitId`) | 79 |
| Período histórico coberto | 2026-03-14 → 2026-05-16 (~63 dias) |
| Partições | 1 (2026-05-16) |

**Parâmetros do endpoint:** `GET /api/unidades/historico` — apenas `page` e
`pageSize`. **Sem filtro de data.** Cada run extrai toda a história disponível.

**Diferença entre os 2 runs:**
- Run 1 (09:03): 75.325 eventos únicos
- Run 2 (09:12): 75.334 eventos únicos
- Diferença: **9 eventos novos** ocorridos nos ~9 minutos entre runs

Os 75.325 eventos do run 1 estão 100% contidos no run 2. O run 2 simplesmente
capturou 9 transições de status adicionais que aconteceram na janela entre os dois.

**Distribuição de status (código texto):**

| Status | Linhas | % | Unidades | Significado provável |
|---|---|---|---|---|
| DC | 47.068 | 31% | 79 | Disponível / De Courseio |
| UE | 30.579 | 20% | 79 | — |
| AM | 28.004 | 19% | 79 | — |
| OS | 10.160 | 7% | 73 | Out of Service |
| UG | 6.906 | 5% | 40 | — |
| DP | 6.626 | 4% | 76 | Dispatched (despachada) |
| LN | 5.576 | 4% | 79 | Login |
| AV | 5.576 | 4% | 79 | Available |
| LO | 5.542 | 4% | 79 | Logoff |
| UV | 3.616 | 2% | 38 | Unavailable |
| QE | 1.006 | 1% | 57 | En Route (a caminho) |

**Frequência de transições:**
75.334 eventos únicos / 79 unidades / 63 dias ≈ **15 transições por unidade por
dia** — coerente com um ciclo operacional de login → disponível → despachada →
a caminho → encerrada → disponível → ... → logout.

### Análise

**Mesmo padrão de extração que `ocorrencias_historico`: full dump sem filtro de
data.** O endpoint retorna todo o histórico de transições (63 dias, 79 unidades)
a cada run. Com dois runs em sequência (09:03 e 09:12), o fator é exatamente 2×:
cada evento aparece uma vez por run. Executar mais vezes no dia só multiplicaria
o volume na staging sem agregar dados novos.

**`unidades_historico` é o complemento event-driven de `unidades_ativas`.** Enquanto
`unidades_ativas` captura o estado das unidades a cada 5min (snapshot periódico),
`unidades_historico` registra cada transição de status exatamente quando acontece
(event-driven). Para análises que exigem precisão de segundos — tempo de resposta
ao despacho, janela dispatch → en route → chegada —, este endpoint é a fonte correta.
`unidades_ativas` só consegue resolução de 5min.

**O campo `ActiveUnitHistoryId` (UUID) é a chave natural única.** Diferente das
outras tabelas históricas, aqui cada registro já tem um identificador próprio
gerado pelo CAD, o que torna deduplicação e joins downstream mais seguros.

**79 unidades históricas vs 71 ativas hoje:** 8 unidades aparecem no histórico
mas não estão logadas hoje. Podem ser viaturas em manutenção, fora de operação
ou de turnos que ainda não começaram.

**Status `DC` (31% das transições) é o mais frequente**, provavelmente o estado
"disponível em base" ou "de courseio" — a situação padrão entre chamadas. `DP`
(despachada) e `QE` (a caminho) juntos somam apenas 5% das transições, o que faz
sentido: o tempo em despacho é curto vs o tempo disponível.

**Crescimento projetado:** 75.334 eventos em 63 dias = ~1.196 eventos/dia. Em 1
ano: ~450K eventos. Cada run diário extrai o acumulado → partição do dia cresce
linearmente. Sem preocupações de volume no BigQuery.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia às 03:00 BRT**, idêntico a `ocorrencias_historico`. O endpoint retorna
  toda a história sem filtro de data — aumentar a frequência só cria cópias extras
  do mesmo volume. O dado noturno é de baixo movimento (poucas transições entre
  meia-noite e 03:00), garantindo completude do dia D-1.
- `dump_mode: append` correto. Cada run diário adiciona uma partição na staging
  com o snapshot completo daquele dia.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Cada partição
  = snapshot diário completo. Padrão idêntico a `ocorrencias_historico`.
- Modelos downstream devem sempre ler a **última partição** para o estado mais
  atual, e usar `CreatedTime` para filtrar eventos de um período específico:
  ```sql
  where data_particao = (select max(data_particao) from {{ ref('raw_segur_forca_municipal__unidades_historico') }})
    and date(safe_cast(CreatedTime as timestamp)) = date_sub(current_date, interval 1 day)
  ```
- Para análise de tempo de resposta (P3): cruzar `DP` → `QE` → status de chegada
  por `AssignedAgencyEventId`, filtrando por `CreatedTime`.
- Rodar dbt **1×/dia** após a extração (ex: 04:00 BRT).

---

## qmd

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 170 |
| Runs distintos (`id_hash`) | 1 (pipeline iniciado hoje) |
| QMDs únicos (`Id`) | 170 (fator 1,0×) |
| Partições | 1 (2026-05-16) |

**Parâmetros do endpoint:** `GET /api/qmd` — apenas `page` e `pageSize`. Sem
filtro de data. Cada run retorna o catálogo completo de QMDs.

**Status:**

| Métrica | Valor |
|---|---|
| QMDs ativos (`StatusAtivo = True`) | 80 (47%) |
| QMDs inativos | 90 (53%) |
| QMDs válidos (`StatusValido = True`) | 169/170 |
| QMDs autorizados (`StatusAutorizado = True`) | 169/170 |

**Vigência:**

| Métrica | Valor |
|---|---|
| Vigência mais antiga | 2026-03-04 |
| Vigência mais recente | 2026-08-14 |
| QMDs com vigência futura | 128 (75%) — ainda em vigor ou planejados |

**Distribuição por área (top 5):**

| Área | QMDs | Ativos |
|---|---|---|
| Campo Grande: Estação de Trem x Calçadão | 46 | 7 |
| Presidente Vargas x Campo de Santana x Central do Brasil x Cinelândia | 34 | 29 |
| Rodoviária x Terminal Gentileza x Estação Leopoldina | 29 | 11 |
| Jardim de Alah | 24 | 5 |
| Estações São Francisco Xavier x Afonso Pena | 14 | 8 |

**Lag de criação:** mediana de ~38 dias entre `DataHoraCriacao` e extração —
confirmando que QMDs são planejados semanas antes de entrarem em vigor. O QMD
mais recente foi criado em 2026-05-13 (3 dias atrás).

### Análise

**`qmd` é um catálogo de referência, não dado em tempo real.** Os 170 QMDs
representam todo o inventário de missões planejadas — ativos, inativos e futuros.
O conteúdo muda lentamente (novos QMDs criados por planejadores; vigências expiram).
Não há eventos operacionais aqui — apenas o plano.

**Diferença essencial de `qmd` para `qmd_ativos`:** `qmd` = catálogo completo
(170 registros, todos os status); `qmd_ativos` = subconjunto com `StatusAtivo=True`
(80 registros, o planejamento vigente). Para o dashboard CompStat, usa-se
`qmd_ativos`. Para análise histórica de "quais QMDs existiam no sistema em D-X",
usa-se `qmd`.

**A tabela tem apenas 170 linhas.** É o menor dataset desta pipeline. O custo de
extrair o catálogo completo a cada run é negligenciável. Não há problema algum de
volume, duplicação ou acúmulo.

**Para CompStat:** o `qmd` serve como lookup — cruzar `Id` com `qmd_detalhes`
(que tem a geometria WKT de cada missão) e com `qmd_servicos` (que liga QMD ↔
viatura). A análise de aderência (P3) precisa saber quais QMDs estavam ativos em
determinada data → usar `DataVigenciaInicio` e `DataVigenciaFim`.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia** é suficiente e apropriado. O catálogo de QMDs muda apenas quando
  planejadores criam ou encerram missões — eventos raros. Extrações mais frequentes
  são desperdício puro para um catálogo estático de 170 registros.
- Se a pipeline atual roda com frequência maior (ex: 15–30 min como sugerido na
  documentação para `qmd_ativos`), não há problema prático dado o volume mínimo,
  mas 1×/dia captura tudo o que é necessário.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Para análise
  CompStat semanal é necessário saber quais QMDs estavam ativos em cada dia
  específico — em particular na primeira segunda-feira de cada semana (referência
  do ciclo) e dia a dia para verificar aderência. Um QMD ativo em D-7 pode ter
  expirado em D; sem o snapshot diário, essa informação se perde. A partição de
  cada dia = catálogo completo naquele momento, permitindo consultas point-in-time.
- Modelos downstream que precisam do estado de uma data específica:
  ```sql
  where data_particao = '<data_referencia>'
  ```
- Rodar dbt **1×/dia** após a extração.

---

## qmd_ativos

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 80 |
| Runs distintos (`id_hash`) | 1 |
| QMDs únicos (`Id`) | 80 (fator 1,0×) |
| Partições | 1 (2026-05-16) |

Extração realizada às 09:06 UTC — 1 minuto após o run de `qmd` (09:05), rodando
em sequência no mesmo pipeline.

**QMDs ativos por área:**

| Área | QMDs ativos | Vigência |
|---|---|---|
| Presidente Vargas x Campo de Santana x Central do Brasil x Cinelândia | 29 | até 2026-07-17 |
| Rodoviária x Terminal Gentileza x Estação Leopoldina | 11 | até 2026-06-17 |
| Estações São Francisco Xavier x Afonso Pena | 8 | até 2026-07-28 |
| Campo Grande: Estação de Trem x Calçadão | 7 | até 2026-07-28 |
| Praia de Botafogo x Rua Marquês de Abrantes | 7 | até 2026-08-12 |
| Base Litorânea | 5 | até 2026-08-14 |
| Jardim de Alah | 5 | até 2026-07-23 |
| Demais áreas | 8 | — |

**Duração dos QMDs:** mediana de 93 dias (~13 semanas) — ciclo trimestral. As
missões planejadas não mudam semana a semana; renovam-se a cada ~3 meses.

### Análise

**`qmd_ativos` é `qmd` filtrado por `StatusAtivo = True`.** Estrutura idêntica,
volume menor (80 vs 170 linhas). Não há dado novo aqui que não exista em `qmd`
— a distinção é operacional: `qmd_ativos` é o endpoint de "planejamento vigente
agora", otimizado para o dashboard em tempo real.

**O ciclo de vigência é trimestral, não semanal.** Os QMDs ativos hoje valem até
junho–agosto de 2026. Isso tem uma implicação importante para o CompStat: a análise
semanal de aderência (P3) compara a execução de cada semana contra o **mesmo
conjunto de QMDs** — que muda apenas a cada ~3 meses. A variação semana a semana
vem dos dados de execução (`unidades_historico`, `unit_positions`), não dos QMDs.

**Redundância controlada com `qmd`:** ter ambas as tabelas não é problema —
o overhead de extrair 80 linhas adicionais é insignificante. Downstream, `qmd_ativos`
serve para queries de estado atual (dashboard); `qmd` com filtro `data_particao =
'<data>'` serve para análise histórica de quais planos estavam vigentes.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia** suficiente pelo mesmo raciocínio de `qmd`. O planejamento muda
  raramente (ciclo trimestral). Extrações mais frequentes não agregam valor.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`**, pelo mesmo
  motivo de `qmd`: o snapshot diário permite saber quais QMDs estavam ativos em
  qualquer segunda-feira de referência do ciclo CompStat.
- Em modelos mart, preferir `qmd` + filtro `StatusAtivo = True` quando for preciso
  cruzar dados históricos — evita dependência de duas tabelas com estrutura idêntica.
  Usar `qmd_ativos` apenas para queries de estado atual no dashboard.

---

## qmd_detalhes

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 170 |
| Runs distintos (`id_hash`) | 1 |
| QMDs únicos (`qmdId`) | 170 (fator 1,0×) |
| Total de missões (somado do JSON) | 1.403 |
| Média de missões por QMD | 8,3 |
| Mediana / máximo de missões | 5 / 23 |
| Tamanho total do JSON `missoes` | 4,78 MB |
| Média de JSON por QMD | 28,8 KB |
| Partições | 1 (2026-05-16) |

**Tipos de missão encontrados:**

| Tipo | Documentado na API |
|---|---|
| PB | Sim — Ponto de Baseamento (POINT) |
| PTR | Sim — Patrulhamento (LINESTRING) |
| RF | Sim — Ronda a Pé (POLYGON) |
| DS | **Não** — tipo não documentado |
| SP | **Não** — tipo não documentado |
| SV | **Não** — tipo não documentado |

**Estrutura do campo `missoes` (JSON array, mantido como string no dbt):**
```
missoes[i]
  .missaoId
  .tipo           (PB / PTR / RF / DS / SP / SV)
  .roteiro
  .horaInicio / .horaFim
  .geometriaWkt   (POINT / LINESTRING / POLYGON em WKT)
  .servicos[j]
    .servicoId
    .nome          (id_unidade da viatura alocada — formato TIPO##-BASE)
    .dias          (JSON array: ["seg","ter",...])
    .execucoes[k]
      .dataHoraInicio
      .dataHoraFim
```

### Análise

**`qmd_detalhes` é o dataset geoespacial central do projeto CompStat.** É a única
fonte que contém, para cada QMD, a geometria exata de onde cada viatura deve estar
(`geometriaWkt`) e quando deve estar lá (`execucoes[].dataHoraInicio`). Sem essa
tabela, a análise de aderência (P3) não pode ser feita.

**Mesmo padrão de extração que `qmd`: full dump, 1 linha por QMD, sem filtro de
data.** O pipeline itera sobre os 170 IDs de QMD e chama `/api/qmd/{id}` para
cada um, consolidando os resultados em 170 linhas. Volume total de 4,78 MB por run
— completamente negligenciável.

**O campo `missoes` é mantido como JSON string no modelo dbt.** A lógica de
unnesting (missoes → servicos → execucoes) fica para modelos mart — correto,
pois o unnesting triplica as linhas e adiciona complexidade que não pertence à
camada raw.

**3 tipos de missão não documentados: DS, SP, SV.** A documentação da API descreve
apenas PB, PTR e RF. Os tipos DS, SP e SV existem nos dados mas sem definição
oficial. Precisam ser confirmados com a equipe da GM antes de incluir interpretações
em modelos mart.

**Para CompStat P3 (aderência):** o cruzamento crítico é:
`qmd_detalhes.missoes[i].geometriaWkt` × `unit_positions.latitude/longitude`
× janela `horaInicio`–`horaFim` × `servicos[j].nome` (= `id_unidade`).
Isso responde: "a viatura estava dentro do polígono/linha durante o horário previsto?"

**Snapshot diário é essencial:** assim como `qmd`, o conteúdo muda quando QMDs
são editados (ajuste de geometria, horário, viaturas alocadas). O snapshot de cada
dia preserva o planejamento vigente naquele momento.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia**, preferencialmente logo após o run de `qmd` (que já determina quais
  IDs existem). O volume total (4,78 MB, 170 requests sequenciais) é pequeno e
  executa em menos de 1 minuto.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`**, pelo mesmo
  motivo de `qmd`: snapshot diário necessário para análise histórica de aderência.
- O campo `missoes` (JSON) deve permanecer como string na camada raw. O unnesting
  em 3 níveis (missões → serviços → execuções) fica em modelos mart.
- Confirmar com a GM os tipos DS, SP, SV antes de mapear em mart.
- Rodar dbt **1×/dia** após a extração.

---

## qmd_kml

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 1.276 |
| Runs distintos (`id_hash`) | 1 |
| QMDs únicos (`qmd_id`) | 170 (fator 1,0×) |
| Partições | 1 (2026-05-16) |

O pipeline parseia o arquivo KML binário de cada QMD e armazena uma linha por
feature geoespacial — não o binário bruto (ao contrário do que a documentação
da API indicava como "não ingerir no BigQuery").

**Estrutura do KML parseado — folders:**

| Folder | Features | QMDs |
|---|---|---|
| `QMD` | 170 | 170 — 1 feature por QMD (boundary da área de atuação) |
| `Missões` | 1.106 | 169 — geometrias individuais das missões |

**Distribuição por tipo de geometria:**

| `geometry_type` | Features | QMDs |
|---|---|---|
| Point | 560 | 170 |
| LineString | 461 | 106 |
| Polygon | 255 | 167 |

**Colunas disponíveis:** `qmd_id`, `kml_folder`, `name`, `description`,
`geometry_wkt`, `geometry_type`, `extended_data`.

### Análise

**`qmd_kml` adiciona duas informações que `qmd_detalhes` não tem:**
1. **Boundary do QMD** — o folder `QMD` tem 1 feature por QMD que representa
   a área geográfica total de atuação. Essa geometria não existe em `qmd_detalhes`,
   que só tem as geometrias das missões individuais.
2. **Metadados de feature** — `name`, `description` e `extended_data` enriquecem
   cada geometria com informação textual do KML.

**Overlap com `qmd_detalhes`:** as 1.106 features do folder `Missões` correspondem
às geometrias de `qmd_detalhes.missoes[].geometriaWkt` (1.403 entradas — a diferença
pode ser missões sem geometria no KML ou missões de tipos DS/SP/SV sem representação
espacial). Para análise de aderência P3, `qmd_detalhes` é preferível por incluir
horários e viaturas alocadas junto com a geometria.

**Uso recomendado:** `qmd_kml` é a fonte para o **mapa de áreas de atuação**
(boundaries de cada QMD) e para visualizações geográficas ad-hoc. Não é a fonte
primária para análise de aderência — use `qmd_detalhes` para isso.

**Prioridade baixa para CompStat**, conforme a própria documentação da API. O dado
é útil mas não é bloqueante para as perguntas P1–P4.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia**, junto com `qmd` e `qmd_detalhes`. O volume é mínimo (1.276 linhas)
  e a extração ocorre no mesmo loop de IDs de QMD que `qmd_detalhes`.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`**, pelo mesmo
  motivo dos demais QMD: snapshot diário preserva as geometrias de vigência anterior
  para análise histórica. Se o boundary de um QMD for ajustado, o snapshot do dia
  anterior reflete o que estava em vigor naquela data.
- Rodar dbt **1×/dia** após a extração.

---

## qmd_plano

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 18 |
| Runs distintos (`id_hash`) | 1 |
| Planos únicos (`Id`) | 18 (fator 1,0×) |
| Partições | 1 (2026-05-16) |

**Todos os 18 planos — histórico completo:**

| Id | Nome | Início | Fim | Situação |
|---|---|---|---|---|
| 14 | (TESTE) Semana 01/03 a 07/03 | 2026-03-01 | 2026-03-08 | Teste |
| 15 | (TESTE) Semana 08/03 a 14/03 | 2026-03-08 | 2026-03-15 | Teste |
| 16 | Força Municipal - Semana 1 | 2026-03-15 | 2026-03-22 | Encerrado |
| 17 | Força Municipal - Semana 2 | 2026-03-22 | 2026-03-29 | Encerrado |
| 18 | Força Municipal - Semana 3 | 2026-03-29 | 2026-04-05 | Encerrado |
| 19 | Força Municipal - Semana 4 | 2026-04-05 | 2026-04-12 | Encerrado |
| 20 | Força Municipal - Semana 5 | 2026-04-12 | 2026-04-19 | Encerrado |
| 21 | Força Municipal - Semana 6 | 2026-04-19 | 2026-04-26 | Encerrado |
| 22 | Força Municipal - Semana 7 | 2026-04-26 | 2026-05-03 | Encerrado |
| 23 | Base Oeste - Semana 1 | 2026-05-03 | 2026-05-10 | Encerrado |
| 24 | Base Litorânea - Semana 1 | 2026-05-03 | 2026-05-10 | Encerrado |
| 25 | Base Norte - Semana 1 | 2026-05-03 | 2026-05-10 | Encerrado |
| 26 | Base Norte - Semana 2 | 2026-05-10 | 2026-05-17 | Em curso |
| 27 | Base Oeste - Semana 2 | 2026-05-10 | 2026-05-17 | Em curso |
| 28 | Base Litorânea - Semana 2 | 2026-05-10 | 2026-05-17 | Em curso |
| 29 | Base Norte - Semana 3 | 2026-05-17 | 2026-05-24 | Futuro |
| 30 | Base Oeste - Semana 3 | 2026-05-17 | 2026-05-24 | Futuro |
| 31 | Base Litorânea - Semana 3 | 2026-05-17 | 2026-05-24 | Futuro |

### Análise

**`qmd_plano` é o calendário oficial do ciclo CompStat.** Cada linha é uma semana
operacional — a unidade de tempo de toda a análise P1–P4. O `Id` de cada plano é
a chave de ligação com `qmd_servicos` (que vincula QMD ↔ viatura ↔ dias de
execução por plano).

**Mudança estrutural confirmada em 2026-05-03:** até a semana 7 (Apr 26 – May 3),
havia 1 plano unificado para toda a Força Municipal. A partir da semana de May 3,
o planejamento foi dividido em 3 planos paralelos por base operacional (Norte,
Oeste, Litorânea). Isso reflete uma descentralização do comando — cada base passou
a ter seu próprio ciclo de análise semanal. Qualquer query que agregue todas as
semanas precisa tratar este break de estrutura (1 plano antes de May 3, 3 planos
por semana depois).

**Planos futuros já estão criados:** as semanas de May 17–24 (IDs 29–31) já
existem no sistema, criados antes do início do período. O planejamento é feito
com pelo menos 1 semana de antecedência.

**18 linhas é o menor dataset de toda a pipeline**, mas é a tabela de maior
valor semântico: sem o `qmd_plano`, não é possível alinhar nenhuma análise ao
ciclo semanal de referência do CompStat.

**O ciclo é domingo → sábado** (ou equivalente), duração fixa de 7 dias para
todos os planos.

### Recomendações

**Pipeline — Prefect:**
- **1×/dia** é mais que suficiente. Novos planos são criados com pelo menos 1
  semana de antecedência. O conteúdo de um plano existente raramente muda após
  sua criação. Extrair 18 linhas diariamente tem custo zero.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Snapshot
  diário necessário para rastrear quando cada plano foi criado e qual era o plano
  vigente em qualquer segunda-feira de referência passada.
- Em modelos mart, usar `SemanaReferenciaInicio` e `SemanaReferenciaFim` para
  cruzar qualquer data com o plano correto:
  ```sql
  where '<data_analise>' between semana_referencia_inicio and semana_referencia_fim
  ```
- Atenção ao break de estrutura: antes de 2026-05-03 o join é 1:1 (data → plano);
  após, é 1:3 (data → 3 planos por base). Modelos mart devem filtrar por base ou
  agregar explicitamente.

---

## qmd_servicos

### Dados coletados

**Visão geral da tabela:**

| Métrica | Valor |
|---|---|
| Total de linhas | 489 |
| Runs distintos (`id_hash`) | 1 |
| Serviços únicos (`Id`) | 489 (fator 1,0×) |
| QMDs com alocação | 95 de 170 (56%) |
| Planos distintos | 16 (IDs 16–31, excluindo os 2 de teste) |
| Unidades distintas | 76 |
| Partições | 1 (2026-05-16) |

**Alocações por plano:**

| IdPlano | Semana | Alocações | QMDs | Unidades |
|---|---|---|---|---|
| 16–17 | Mar 15 – Mar 29 | 20 cada | 20 | 20 |
| 18–22 | Mar 29 – Mai 03 | 44–57 | cresce | cresce |
| 23 / 25 / 26 / 27 / 29 / 30 | Base Oeste e Norte | 7–8 cada | 7–8 | 7–8 |
| 24 / 28 / 31 | Base Litorânea | 42–58 | 42–57 | 42–58 |

**Dias da semana:** virtualmente todas as 489 alocações cobrem os 7 dias
(dom–sab). Apenas 1 alocação exclui segunda-feira. O planejamento não é
dia-específico — a granularidade é semanal.

**Unidades mais alocadas (top 3):** todas da Base Litorânea, cada uma alocada
em 10 planos consecutivos sempre para o **mesmo QMD** — alocação estável e
persistente ao longo das semanas.

### Análise

**`qmd_servicos` é a tabela de junção central do modelo de planejamento:** resolve
o relacionamento QMD ↔ Unidade ↔ Plano. Sem ela, não é possível responder "qual
viatura deveria estar em qual missão em qual semana".

**A alocação de unidades é altamente estável.** As unidades da Base Litorânea,
por exemplo, aparecem no mesmo QMD por 10 semanas consecutivas. Isso confirma que
o planejamento é um "standing order" — a mesma viatura patrulha a mesma área semana
após semana, salvo exceções. Para CompStat P3, isso é conveniente: o plano de
aderência raramente muda entre semanas consecutivas, então os dados de execução é
que determinam a variação.

**75 QMDs (44%) não têm nenhuma alocação de unidade.** Esses QMDs existem no
catálogo mas não têm viatura designada — podem ser missões planejadas ainda sem
escala, QMDs históricos encerrados antes de receberem alocação, ou tipos de missão
que não requerem viatura específica (ex: DS, SP, SV dos tipos não documentados).

**A Base Litorânea domina em volume:** 42–58 alocações vs 7–8 para Norte e Oeste.
Isso é coerente com o dado de `unidades_ativas` — a Base Litorânea tinha mais
unidades logadas no sistema e entrou em operação mais tardiamente no turno.

**Join model para CompStat P3:**
```sql
qmd_plano           -- semana de referência
  → qmd_servicos    -- (IdPlano) → qual unidade em qual QMD
  → qmd_detalhes    -- (IdQmd)   → geometria + horário da missão
  → unidades_historico / unit_positions  -- execução real da unidade
```

### Recomendações

**Pipeline — Prefect:**
- **1×/dia** suficiente. A tabela de serviços muda apenas quando a GM realoca
  viaturas entre QMDs — evento semanal no máximo. 489 linhas por extração, custo
  negligenciável.

**dbt — Materialização:**
- **Manter `incremental` + `insert_overwrite` por `data_particao`.** Snapshot
  diário preserva o estado das alocações em cada data — necessário para reconstruir
  o plano vigente de qualquer semana passada (qual unidade estava alocada a qual
  QMD naquela semana específica).
- O campo `Dias` (JSON array de strings) deve ser mantido como string na camada
  raw e parseado em mart via `JSON_EXTRACT_STRING_ARRAY`. Como virtualmente todos
  os serviços cobrem os 7 dias, a lógica de filtragem por dia raramente será usada
  na prática — mas deve existir para cobrir exceções.
- Rodar dbt **1×/dia** após a extração.

---
