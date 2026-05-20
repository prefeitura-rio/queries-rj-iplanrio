# Modelos Raw — Força Municipal (HxGN OnCall / CIVITAS)

Modelos de materialização das tabelas de dump da pipeline `rj_segur__forca_municipal`.

**Fonte:** API HxGN OnCall (CIVITAS/CORIO) — sistema de gestão de ocorrências e unidades da Guarda Municipal do Rio de Janeiro.
**Staging:** `rj-iplanrio.brutos_forca_municipal_staging`
**Destino:** `rj-segur.brutos_forca_municipal`
**Marts:** `rj-segur.forca_municipal` — ver `models/mart/segur/forca_municipal/`

---

## Índice

- [Convenções de nomeação de arquivos](#convenções-de-nomeação-de-arquivos)
- [Estratégias de materialização](#estratégias-de-materialização)
- [Padronização de colunas](#padronização-de-colunas)
- [Tipos de dados por campo da API](#tipos-de-dados-por-campo-da-api)
- [Consistência entre modelos](#consistência-entre-modelos)
- [Campos descartados](#campos-descartados)
- [Modelos raw](#modelos)
  - `unit_positions` · `unidades_historico` · `ocorrencias_historico` · `ocorrencias_ativas_v2`
  - `qmd_plano` · `qmd` · `qmd_servicos` · `qmd_missoes`
  - `qmd_geometria_kml` · `qmd_geometria_bases` · `qmd_geometria_missoes_rotas` · `qmd_geometria_missoes_areas` · `qmd_geometria_missoes_pontos` · `qmd_geometria_kml_outros`
  - `monitoramento_qualidade`
- [Marts](#marts-rj-segurforca_municipal)
  - `unidade` · `qmd_missoes` · `ocorrencias` · `turnos`

---

## Convenções de nomeação de arquivos

```
raw_segur_forca_municipal__<table_name>.sql
raw_segur_forca_municipal__<table_name>.yml
```

O `alias` no config de cada modelo deve ser `<table_name>` (sem o prefixo).

**Regra:** o bloco `config:` pertence exclusivamente ao arquivo `.sql`. O `.yml` não deve
conter `config:` — qualquer configuração duplicada no YML sobrescreve silenciosamente o
SQL e é fonte de inconsistência.

---

## Estratégias de materialização

Existem três padrões no dataset, cada um adequado a uma semântica de fonte distinta.

### 1. `table` — substituição total por run

Usado quando a API retorna o estado completo a cada coleta e não há crescimento incremental
confiável. O modelo é sobrescrito integralmente a cada run.

```python
materialized="table"
```

**Modelos:** `ocorrencias_historico`, `unidades_historico`, `monitoramento_qualidade`.

### 2. `incremental / insert_overwrite` — log de eventos por data

Usado para tabelas de log que crescem por data de negócio. Cada partição representa um
dia fechado de operação. O watermark usa a própria tabela destino para evitar lookback fixo:

```sql
{% if is_incremental() %}
    where safe_cast(data_particao as date) >= (
        select max(data_particao) from {{ this }}
    )
{% endif %}
```

- `>=` (não `>`): sempre reprocessa a última partição para capturar late arrivals.
- `insert_overwrite`: substitui as partições tocadas de forma atômica — sem duplicatas,
  sem necessidade de `unique_key`.

```python
materialized="incremental",
incremental_strategy="insert_overwrite",
partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"},
```

**Modelos:** `unit_positions`.

> **Atenção:** `unit_positions` é o único modelo do dataset onde `data_particao` representa
> uma data de **negócio** (D-1, parâmetro passado à API). Em todos os outros modelos,
> `data_particao` é a data do run da pipeline (controle de ingestão).

### 3. `incremental / merge` — deduplicação por `id_hash`

Usado quando a API retorna snapshots do estado atual de entidades (QMDs, missões,
ocorrências ativas). O modelo colapsa snapshots idênticos: uma linha por versão única
de conteúdo (`id_hash`), com `first_seen`/`last_seen` indicando o intervalo em que
aquela versão foi observada.

```python
materialized="incremental",
incremental_strategy="merge",
unique_key="id_hash",
partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"},
```

**Lógica de merge:**
- **Insert:** novo `id_hash` que não existe ainda → primeira vez que este conteúdo aparece.
- **Update:** `id_hash` já existe → atualiza `last_seen`, `updated_at` e `data_particao`.
- `first_seen` nunca é atualizado após o insert — é imutável por design.

**Modelos:** família QMD (`qmd`, `qmd_plano`, `qmd_servicos`, `qmd_missoes`,
`qmd_geometria_kml` e derivados), `ocorrencias_ativas_v2`.

---

## Padronização de colunas

Todo modelo deve organizar as colunas na seguinte ordem, com comentários de bloco.

### 1. Metadados da pipeline

Sempre os primeiros campos. Gerados pela pipeline, não pela API.

**Modelos `table` e `insert_overwrite`:**

```sql
-- metadados
{{ padronize_id('id_hash') }} as id_hash,
safe_cast(updated_at as datetime) as updated_at,
-- partição ao final (ver seção 5)
```

**Modelos `merge` (deduplicado):**

```sql
-- metadados
{{ padronize_id('id_hash') }} as id_hash,
safe_cast(updated_at as datetime) as first_seen,   -- MIN(updated_at) no merge
safe_cast(updated_at as datetime) as last_seen,    -- MAX(updated_at) no merge
safe_cast(updated_at as datetime) as updated_at,   -- igual a last_seen, por compatibilidade
-- data_particao ao final (ver seção 5)
```

| Campo | Tipo | Descrição |
|---|---|---|
| `id_hash` | `string` | Hash MD5 calculado por linha sobre colunas da API (excluindo `updated_at`). Mesmo conteúdo = mesmo hash entre runs. |
| `first_seen` | `datetime` | Primeira coleta em que esta versão (`id_hash`) foi observada. Imutável após insert. Só presente em modelos merge. |
| `last_seen` | `datetime` | Última coleta em que esta versão foi observada. Atualizado a cada run enquanto o conteúdo não muda. Só presente em modelos merge. |
| `updated_at` | `datetime` | Timestamp da coleta pela pipeline (America/Sao_Paulo). Igual a `last_seen` nos modelos merge. |

### 2. Identificadores (FKs para outras entidades)

Campos que referenciam entidades externas: agência, unidade, funcionário, zona geográfica, grupo.
O ID **primário da própria entidade** descrita na tabela vai em `-- dados`, não aqui.

> **Regra:** todo campo que é um identificador deve obrigatoriamente ter nome `id_*`,
> independentemente de ser numérico ou alfanumérico, e usar `{{ padronize_id('col') }}`.
> Códigos curtos de sigla/classificação usam `upper(safe_cast(...))`.

```sql
-- identificadores (FKs para outras entidades)
upper({{ padronize_id('AgencyId') }}) as id_agencia,        -- sigla → UPPER
upper({{ padronize_id('UnitId') }}) as id_unidade,          -- código/sigla → padronize + UPPER
{{ padronize_id('EmployeeId') }} as id_funcionario,         -- numérico → remove .0
```

### 3. Dados

Campos de negócio na seguinte sub-ordem:

1. **ID primário da entidade** — o identificador único da linha (ex. `id_ocorrencia`,
   `id_unidade`). Abre o bloco `-- dados` para que o contexto da entidade seja lido antes
   de seus atributos.
2. **Tipo / subtipo / descrição** — código + descrição do tipo da entidade, logo após o ID.
3. **Datas/horas** — timestamps convertidos para `America/Sao_Paulo`.
4. **Status, flags e demais atributos** — prioridade, status, booleanos, contagens, texto livre.

```sql
-- dados
{{ padronize_id('AgencyEventId') }} as id_ocorrencia,
upper(safe_cast(AgencyEventTypeCode as string)) as tipo_ocorrencia_codigo,
{{ proper_br('safe_cast(AgencyEventTypeCodeDesc as string)') }} as tipo_ocorrencia_descricao,

datetime(safe_cast(CreatedTime as timestamp), 'America/Sao_Paulo') as data_hora_criacao,
datetime(safe_cast(ClosingTime as timestamp), 'America/Sao_Paulo') as data_hora_encerramento,

{{ padronize_id('StatusCode') }} as id_status,
safe_cast(IsOpen as bool) as indicador_aberta,
{{ padronize_id('Priority') }} as prioridade,
```

**Regras de nomenclatura:**

| Caso | Macro/função | Nome resultante |
|---|---|---|
| ID de qualquer entidade | `{{ padronize_id('col') }}` | sempre `id_*` — remove `.0` de floats |
| Código / sigla curta | `upper(safe_cast(col as string))` | ex. `"pog01"` → `"POG01"` |
| Nome / endereço descritivo | `{{ proper_br('safe_cast(col as string)') }}` | ex. `"RIO DE JANEIRO"` → `"Rio de Janeiro"` |
| Timestamp com fuso (`+00:00` ou `-03:00`) | `datetime(safe_cast(col as timestamp), 'America/Sao_Paulo')` | converte para BRT |
| Datetime sem fuso (`updated_at`) | `safe_cast(col as datetime)` | já em BRT, sem conversão |

### 4. Campos espaciais

Sempre nesta ordem: `latitude`, `longitude`, `geometry`. O campo `geometry` **deve sempre
ser criado** quando houver coordenadas, usando `ST_GEOGPOINT(longitude, latitude)`.

> Atenção: `ST_GEOGPOINT` recebe `(longitude, latitude)` — longitude primeiro.

```sql
-- espacial
safe_cast(Latitude as float64) as latitude,
safe_cast(Longitude as float64) as longitude,
st_geogpoint(
    safe_cast(Longitude as float64),
    safe_cast(Latitude as float64)
) as geometry,
```

### 5. Partição

Sempre o último campo.

```sql
-- partição
safe_cast(data_particao as date) as data_particao,
```

---

## Tipos de dados por campo da API

**Regra geral:** só usar tipo numérico (`int64`, `float64`) quando o campo for usado em
operações matemáticas (soma, média, subtração). Códigos, níveis e status permanecem
como `string` mesmo que seus valores sejam numéricos.

| Tipo na API | Tipo BigQuery | Cast |
|---|---|---|
| ID (qualquer origem) | `string` | `{{ padronize_id('col') }}` — remove `.0` de floats |
| Código / sigla curta | `string` | `upper(safe_cast(col as string))` |
| Nome / endereço descritivo | `string` | `{{ proper_br('safe_cast(col as string)') }}` |
| Código numérico (status, nível) | `string` | `{{ padronize_id('col') }}` — sem operações matemáticas |
| **Número de sequência** (ex. `numero_revisao`) | `int64` | `safe_cast({{ padronize_id('col') }} as int64)` — ordenação numérica |
| Duração / contagem (operações matemáticas) | `int64` | `safe_cast(col as int64)` |
| Float que vem como `"1.0"` antes de int | `int64` | `safe_cast(safe_cast(col as float64) as int64)` |
| Coordenadas geográficas | `float64` | `safe_cast(col as float64)` |
| `boolean` | `bool` | `safe_cast(col as bool)` |
| `string` ISO 8601 com tz (ex. `+00:00`, `-03:00`) | `datetime` BRT | `datetime(safe_cast(col as timestamp), 'America/Sao_Paulo')` |
| `string` datetime sem tz (`updated_at`) | `datetime` | `safe_cast(col as datetime)` — já em BRT |
| `string` data (`YYYY-MM-DD`) | `date` | `safe_cast(col as date)` |
| JSON serializado | `string` | mantém como `string` |
| Array serializado como JSON | `string` | mantém como `string` |

> Todos os campos da staging chegam como `STRING` (limitação do Parquet gerado pela pipeline).
> Use sempre `safe_cast` para evitar quebra do modelo em caso de valor inválido.

---

## Consistência entre modelos

### Mesmo nome para a mesma informação

Campos que representam o mesmo conceito em tabelas diferentes **devem ter exatamente o
mesmo nome**, independentemente do nome original na API. Isso elimina ambiguidade nos
joins e torna o mart previsível.

Exemplos consolidados neste dataset:

| Campo | Tabelas | Significado |
|---|---|---|
| `id_unidade` | `unit_positions`, `qmd_servicos`, `qmd_missoes`, `unidades_historico` | Identificador da viatura no formato `TIPO##-BASE` |
| `base_operacional` | `unit_positions`, `qmd_servicos`, `qmd_missoes`, `unidades_historico` | Base operacional (NORTE / OESTE / LITORANEA) |
| `tipo_unidade` | `unit_positions`, `qmd_servicos`, `qmd_missoes`, `unidades_historico` | Tipo da viatura (POG / VTR / MOT / TRP / SUP / CUST) |
| `indicador_desvio_missao` | `ocorrencias_historico`, `ocorrencias_ativas_v2` | Se a ocorrência é um Desvio de Missão (DM) |
| `indicador_evento_operacional` | `ocorrencias_historico`, `ocorrencias_ativas_v2` | Se é DM ou AGD — filtro-base de P1 |

> Em `qmd_plano`, o equivalente de `base_operacional` chama-se `area` porque é o campo
> primário da tabela — para joins, usar `qmd_plano.area = <tabela>.base_operacional`.

### Números de sequência → `int64`

Campos que representam uma **sequência ordinal** usada para ordenação (ex. `numero_revisao`)
devem ser convertidos para `int64`, mesmo que o README geral recomende `string` para
códigos numéricos. A distinção é:

- **Código numérico** (ex. `id_status = "7"`): sem ordenação relativa, permanece `string`.
- **Número de sequência** (ex. `numero_revisao = 10`): ordenação define semântica
  ("última revisão"). Manter como `string` causa ordenação lexicográfica silenciosamente
  errada (`"9" > "10"`).

```sql
-- correto para números de sequência
safe_cast({{ padronize_id('RevisionNumber') }} as int64) as numero_revisao
```

---

## Campos descartados

Os campos `ano_particao` e `mes_particao` são descartados em todos os modelos por serem
redundantes com `data_particao`.

---

## Modelos

### `unit_positions`

**Grain:** uma leitura GPS por unidade × timestamp | **Materialização:** incremental / insert_overwrite | **Joins:** `id_unidade` → `qmd_servicos`, `unidades_historico`

Log completo de posições GPS de todas as viaturas ativas, dia D-1. Frequência de leitura
~17–44s por unidade. Único modelo do dataset onde `data_particao` é data de negócio
(parâmetro passado à API), não data do run da pipeline. `hora_leitura` (TIME) e `geometry`
(GEOGRAPHY) já derivados para eliminar casts em queries de conformidade espacial.

---

### `unidades_historico`

**Grain:** evento de mudança de status por unidade | **Materialização:** table | **Joins:** `id_unidade` → `unit_positions`, `qmd_servicos`; `id_ocorrencia_atribuida` → `ocorrencias_historico`

Histórico de eventos de status, posição e atribuição das viaturas. Os campos
`tempo_total_ocorrencias`, `tempo_total_indisponivel`, `tempo_total_disponivel_estacao`
e `total_ocorrencias` são **contadores crescentes desde o logon** — o valor correto de um
turno está apenas no último evento. `indicador_despachada` e `data_logon` são campos
derivados para facilitar filtros e agrupamentos por turno.

---

### `ocorrencias_historico`

**Grain:** revisão por ocorrência (`id_ocorrencia × numero_revisao`) | **Materialização:** table | **Joins:** `id_ocorrencia` → `ocorrencias_ativas_v2`

Histórico de todas as revisões de ocorrências. Para obter o estado atual de cada
ocorrência, filtrar por `indicador_ultima_revisao = TRUE` — equivale a
`QUALIFY ROW_NUMBER() OVER (PARTITION BY id_ocorrencia ORDER BY numero_revisao DESC) = 1`.
`duracao_ocorrencia_minutos` já calculado. Não tem localização nem TTD/TTER/TTOA — esses
campos estão em `ocorrencias_ativas_v2`. `indicador_evento_operacional` é o filtro-base
de P1: `WHERE NOT indicador_evento_operacional`.

---

### `ocorrencias_ativas_v2`

**Grain:** estado único de ocorrência ativa (`id_hash`) | **Materialização:** incremental / merge | **Joins:** `id_ocorrencia` → `ocorrencias_historico`

Histórico deduplicado de estados de ocorrências ativas. A staging é populada por polling
a cada ~5 min; este modelo colapsa snapshots idênticos — uma linha por `id_hash`, com
`first_seen`/`last_seen` indicando a janela de observação. Contém localização (lat/lon/geometry)
e timeline de despacho (`data_hora_primeiro_despacho`, `data_hora_primeira_unidade_a_caminho`,
`data_hora_primeira_chegada`) e as métricas derivadas `ttd_segundos`, `tter_segundos`,
`ttoa_segundos`. Ocorrências fechadas desaparecem do feed — usar `ocorrencias_historico`
para histórico completo.

---

### `qmd_plano`

**Grain:** plano semanal | **Materialização:** incremental / merge | **Joins:** `id_plano` → `qmd_servicos`

Planos semanais de QMDs. `area` (NORTE / OESTE / LITORANEA) é derivada do campo `nome`
e está NULL para planos da era unificada (antes de 2026-05-03) — usar
`indicador_plano_unificado` para distinguir. `data_semana_referencia_inicio` e `_fim`
como DATE eliminam cast em joins com `unit_positions.data_particao`.

---

### `qmd`

**Grain:** versão de QMD (`id_hash`) | **Materialização:** incremental / merge | **Joins:** `id_qmd` → `qmd_servicos`, `qmd_missoes`, `qmd_geometria_bases`

Quadros de Missões Diárias. Uma linha por versão de conteúdo — mudanças no QMD geram
novo `id_hash`. `indicador_hora_cruza_meia_noite` e `duracao_minutos_qmd` já derivados;
não requerem lógica condicional no downstream.

---

### `qmd_servicos`

**Grain:** alocação unidade × QMD × plano (`id_servico`) | **Materialização:** incremental / merge | **Joins:** `id_plano` → `qmd_plano`; `id_qmd` → `qmd`; `id_servico` → `qmd_missoes`; `id_unidade` → `unit_positions`

Alocações de unidades a QMDs em planos semanais. Elo central do modelo de planejamento:
liga `qmd_plano` → `qmd` → `qmd_missoes`. `tipo_unidade` e `base_operacional` derivados
do `id_unidade`. `dias` é `ARRAY<STRUCT<week_day, week_day_number>>` — em produção 99,8%
dos serviços cobrem os 7 dias.

---

### `qmd_missoes`

**Grain:** missão × serviço (`id_missao × id_servico`) | **Materialização:** incremental / merge | **Joins:** `id_servico` → `qmd_servicos`; `id_missao` → `qmd_geometria_missoes_*`

Missões dos QMDs com unnest inline do JSON de missões do endpoint `/api/qmd/{id}`.
`execucoes` (`ARRAY<STRUCT<data_hora_inicio, data_hora_fim, week_day, week_day_number>>`)
é a fonte autoritativa para joins temporais com `unit_positions` — substitui
`hora_inicio/fim_missao + dias` com uma única condição `BETWEEN`. `geometry` disponível
mas `qmd_geometria_missoes_*` são a fonte autoritativa de geometria.

---

### `qmd_geometria_kml`

**Grain:** feature KML por versão (`id_hash`) | **Materialização:** incremental / merge | **Joins:** `id_missao` → `qmd_missoes`; `id_qmd` → `qmd`

Base model que centraliza todas as transformações do KML exportado pelo sistema QMD.
Cobre todas as pastas conhecidas (`Missões` e `QMD`). Modelos derivados apenas filtram
por `kml_folder` ou `tipo_missao` — nenhuma transformação própria. `servicos`
(`ARRAY<STRING>`) e `roteiro` (prefixo removido via regex) derivados aqui.

---

### `qmd_geometria_bases`

**Grain:** feature KML da pasta `QMD` | **Derivado de:** `qmd_geometria_kml` | **Joins:** `id_qmd` → `qmd`

Pontos físicos (POINT) das bases QMD. Filtro: `kml_folder = 'QMD'`. Alguns QMDs têm
múltiplas features — agregar com `ANY_VALUE(geometry)` quando necessário para evitar
fan-out em joins.

---

### `qmd_geometria_missoes_rotas`

**Grain:** rota de patrulhamento PTR | **Derivado de:** `qmd_geometria_kml` | **Joins:** `id_missao` → `qmd_missoes`

Geometrias LineString de missões de patrulhamento (PTR). Filtro: `tipo_missao = 'PTR'`.
~474 linhas em produção.

---

### `qmd_geometria_missoes_areas`

**Grain:** área de cobertura de missão (RF / SV / SP) | **Derivado de:** `qmd_geometria_kml` | **Joins:** `id_missao` → `qmd_missoes`

Geometrias Polygon de missões de cobertura de área. Filtro: `tipo_missao IN ('RF', 'SV', 'SP')`.
`indicador_geometry_util = FALSE` para polígonos que representam a base inteira (SV/SP
sem subárea e RF genérica > 50 km²) — excluir em análises espaciais de conformidade.
~259 linhas em produção.

---

### `qmd_geometria_missoes_pontos`

**Grain:** ponto base fixo PB | **Derivado de:** `qmd_geometria_kml` | **Joins:** `id_missao` → `qmd_missoes`

Geometrias Point de missões de ponto base (PB). Filtro: `tipo_missao = 'PB'`. ~400 linhas
em produção.

---

### `qmd_geometria_kml_outros`

**Grain:** feature KML não classificada | **Derivado de:** `qmd_geometria_kml`

Catch-all para features que não se encaixam em nenhum dos modelos derivados anteriores.
Deve ter **zero linhas** em produção — monitorado pelo teste singular
`raw_segur_forca_municipal__qmd_kml_outros__vazio.sql`. Linhas aqui indicam novo tipo
de feature KML não mapeado.

---

### `monitoramento_qualidade`

**Grain:** uma linha por tabela do dataset | **Materialização:** table

Painel de frescor e volume para todos os modelos do dataset. Registra `ultima_atualizacao`,
`inicio_historico`, `total_linhas` e `dias_atraso` para cada tabela, com lógica de cálculo
adaptada ao tipo de materialização (`merge_dedup`, `partitioned_incremental`, `table`).
Usado para detectar falhas de pipeline e atrasos de ingestão.

---

## Marts (`rj-segur.forca_municipal`)

Camada analítica sobre os modelos raw. Joins canônicos sem lógica de negócio — decisões
operacionais (tolerâncias, definições de conformidade) pertencem ao CompStat. Localização:
`models/mart/segur/forca_municipal/`.

---

### `unidade`

**Grain:** uma linha por `id_unidade` | **Materialização:** table | **Fonte:** `unidades_historico`

Dimensão estática de viaturas. Estado mais recente por unidade via QUALIFY, eliminando
a necessidade de `GROUP BY + ANY_VALUE` em queries downstream. Fonte autoritativa para
`tipo_unidade` e `base_operacional` — nenhum outro modelo precisa re-derivá-los via regex.

---

### `qmd_missoes`

**Grain:** uma linha por unidade × missão (`id_servico × id_missao`) | **Materialização:** table | **Fontes:** `qmd_plano → qmd_servicos → qmd → qmd_missoes`

Join canônico do planejamento operacional. Substitui a cadeia de 4 tabelas que toda query
de planejamento precisava re-escrever. Cobre: dados do plano semanal, atributos do QMD,
tipo/roteiro/horário de cada missão e dias de execução. Geometrias disponíveis nas tabelas
raw `qmd_geometria_missoes_*` — join por `id_missao`.

---

### `ocorrencias`

**Grain:** uma linha por `id_ocorrencia` (estado final) | **Materialização:** table | **Fontes:** `ocorrencias_historico` LEFT JOIN `ocorrencias_ativas_v2`

Estado final de cada ocorrência, eliminando o `QUALIFY ROW_NUMBER` e o join manual entre
as duas tabelas de ocorrências. Campos de localização e métricas de despacho (TTD / TTER /
TTOA) provêm de `ocorrencias_ativas_v2` — NULL para ocorrências que fecharam antes de
serem capturadas pelo feed. Filtro-base de P1: `WHERE NOT indicador_evento_operacional`.
Filtro P4: `ttd_segundos`, `tter_segundos`, `ttoa_segundos`.

---

### `turnos`

**Grain:** uma linha por `id_unidade × data_logon` | **Materialização:** table | **Fonte:** `unidades_historico`

Consolidação de turno eliminando a necessidade de extrair o valor final dos contadores
acumulados (`tempo_total_ocorrencias`, `tempo_total_indisponivel`, `tempo_total_disponivel_estacao`,
`total_ocorrencias`) via QUALIFY com desempate por `ordem_criacao`. Inclui também
`total_eventos` e `total_eventos_despachada` por agregação. Ponto de entrada natural para
análises de carga e disponibilidade por turno e base operacional.
