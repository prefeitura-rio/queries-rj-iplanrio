# Modelos Raw — Força Municipal (HxGN OnCall / CIVITAS)

Modelos de materialização das tabelas de dump da pipeline `rj_segur__forca_municipal`.

**Fonte:** API HxGN OnCall (CIVITAS/CORIO) — sistema de gestão de ocorrências e unidades da Guarda Municipal do Rio de Janeiro.
**Staging:** `rj-iplanrio.brutos_forca_municipal_staging`
**Destino:** `rj-iplanrio.brutos_forca_municipal`

---

## Convenções de nomeação de arquivos

```
raw_segur_forca_municipal__<table_name>.sql
raw_segur_forca_municipal__<table_name>.yml
```

O `alias` no config de cada modelo deve ser `<table_name>` (sem o prefixo).

---

## Estratégia de materialização

Todos os modelos usam:

```python
materialized="incremental",
incremental_strategy="insert_overwrite",
partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"},
```

O filtro incremental usa a própria tabela destino como watermark, evitando lookback fixo
e garantindo que late arrivals sejam capturados:

```sql
{% if is_incremental() %}
    where safe_cast(data_particao as date) >= (
        select max(data_particao) from {{ this }}
    )
{% endif %}
```

- `>=` (não `>`): sempre reprocessa a última partição para capturar dados que chegaram
  após a última materialização.
- `insert_overwrite`: substitui as partições tocadas de forma atômica — sem duplicatas,
  sem necessidade de `unique_key`.
- Primeiro run: `is_incremental()` é `false`, logo a staging é lida por completo.

---

## Padronização de colunas

Todo modelo deve organizar as colunas na seguinte ordem, com comentários de bloco:

### 1. Metadados da pipeline

Sempre os primeiros campos. Gerados pela pipeline, não pela API.

```sql
-- metadados da pipeline
{{ padronize_id('id_hash') }} as id_hash,
safe_cast(updated_at as datetime) as updated_at,
```

| Campo | Tipo | Descrição |
|---|---|---|
| `id_hash` | `string` | Hash MD5 do conteúdo bruto da resposta da API. Identifica um arquivo Parquet no GCS. Todas as linhas de um mesmo run compartilham o mesmo hash. Usado para deduplicação e monitoramento. |
| `updated_at` | `datetime` | Timestamp da coleta pela pipeline (America/Sao_Paulo). Já armazenado em BRT pela pipeline — não requer conversão. |

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

## Exemplo completo

```sql
with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "minha_tabela") }}
        {% if is_incremental() %}
            where safe_cast(data_particao as date) >= (
                select max(data_particao) from {{ this }}
            )
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id('id_hash') }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,

            -- identificadores (FKs para outras entidades)
            upper({{ padronize_id('AgencyId') }}) as id_agencia,
            upper({{ padronize_id('UnitId') }}) as id_unidade,

            -- dados
            {{ padronize_id('AgencyEventId') }} as id_ocorrencia,
            upper(safe_cast(AgencyEventTypeCode as string)) as tipo_ocorrencia_codigo,
            {{ proper_br('safe_cast(AgencyEventTypeCodeDesc as string)') }} as tipo_ocorrencia_descricao,

            datetime(safe_cast(CreatedTime as timestamp), 'America/Sao_Paulo') as data_hora_criacao,
            datetime(safe_cast(ClosingTime as timestamp), 'America/Sao_Paulo') as data_hora_encerramento,

            {{ padronize_id('StatusCode') }} as id_status,
            safe_cast(IsOpen as bool) as indicador_aberta,
            {{ padronize_id('Priority') }} as prioridade,

            -- espacial
            safe_cast(Latitude as float64) as latitude,
            safe_cast(Longitude as float64) as longitude,
            st_geogpoint(
                safe_cast(Longitude as float64),
                safe_cast(Latitude as float64)
            ) as geometry,

            -- partição
            safe_cast(data_particao as date) as data_particao,
        from source
    )

select *
from renamed
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

## Campos descartados

Os campos `ano_particao` e `mes_particao` são descartados em todos os modelos por serem
redundantes com `data_particao`.
