# Documento relacionado às regras de negócio, mudanças e conversas entre os times da GTIS2, DataLake e SME

# Reunião no dia 01/09/2026:
- Presentes:
  - 
  - Patrick Teixeira (DataLake)
  - Fernanda Scovino (DataLake)
  - Luciano Muniz (DataLake)
  - Fernando Fernandes (GTIS2)
  - Luis Claudio Rodrigues (GTIS2)
  - Gabrielle Freire Domingues (SME)
  - Kamila Ferreira (SME)

# Resumo — Ajustes nas tabelas de frequência acumulada

A reunião teve como objetivo corrigir distorções identificadas nas tabelas **`frequencia_acumulada`** e **`frequencia_acumulada_dias_letivos`**, que apresentavam inconsistências no cálculo de aulas e faltas. Os principais problemas estavam relacionados à agregação dos dados por ano e à diferença entre as regras de cálculo de frequência por **dias lançados pelos professores** e por **dias letivos**.

Também foi identificada a necessidade de permitir a análise histórica dos dados, eliminando a restrição ao ano corrente e incluindo o ano calendário na estrutura das tabelas.

## Problemas identificados

- **`frequencia_acumulada`**
  - Utilizada pela Gabi.
  - Considera as aulas por dias lançados pelos professores, sejam eles letivos ou não.
  - A agregação dos dados sem a separação adequada por ano estava inflando a quantidade de aulas e faltas.

- **`frequencia_acumulada_dias_letivos`**
  - Utilizada pela Kamila.
  - Considera as aulas a partir dos dias letivos.
  - Apresentava problema semelhante de agregação por ano, causando distorções nos resultados.

- A ausência de um filtro/agrupamento adequado por ano fazia com que dados de diferentes períodos fossem contabilizados conjuntamente.

- Também foi identificada uma divergência conceitual entre **"dias letivos"** e **"total de aulas"**, especialmente em situações envolvendo alunos de projetos ou casos em que o SGA realiza a contabilização de aulas de maneira diferente.

## Regras e alterações definidas

### Inclusão do ano calendário

Todas as tabelas de frequência deverão passar a considerar o **ano calendário (`ano_calendario`)**, permitindo a correta segregação dos dados.

Na nova versão das queries:

- `ano_calendario` será obtido a partir de `cal.cal_ano`.
- A coluna deverá estar presente tanto no `SELECT` quanto no `GROUP BY`.
- A granularidade passará de **uma linha por aluno** para **uma linha por aluno/ano**, permitindo a consulta do histórico completo.

### Remoção do filtro do ano corrente

Será removido o filtro:

```sql
cal.cal_ano = EXTRACT(YEAR FROM CURRENT_DATE())
```

Com isso, as tabelas deixarão de apresentar somente os dados do ano corrente e passarão a contemplar o histórico de frequência.

### Consistência entre as tabelas

Foi validado que a **frequência acumulada deve ser derivada da tabela de dias letivos**, garantindo maior precisão e aderência às regras de negócio da Secretaria Municipal de Educação (SME).

### Tratamento de datas futuras

Será mantida, inicialmente, a regra que impede a contabilização de registros com **datas futuras**, evitando que lançamentos indevidos no sistema sejam considerados como frequência.

A necessidade desse filtro deverá ser reavaliada posteriormente.

### Alteração na `frequencia_acumulada_dias_letivos`

Na nova versão da query:

- `ano_calendario` será adicionado nas duas partes do `UNION` e no `GROUP BY` final.
- Será removido o filtro de ano corrente.
- Será removido o filtro:

```sql
CAP.cap_dataFim < CURRENT_DATE()
```

> **Ponto de atenção:** essa última alteração deve ser confirmada com a Gabi antes da materialização, pois pode modificar o que é contabilizado no COC vigente.

- A subquery responsável pelo cálculo manual de faltas será substituída pela leitura direta da **`vw_alunos_aulas`**, que já possui os dados pré-agregados.
- Com isso, a regra de cálculo passa a ser centralizada em uma única view, reduzindo a duplicidade de lógica envolvendo `turma_aula_aluno`, `turma_aula`, `formato_avaliacao`, dias não letivos e faltas abonadas.

## Ações e responsáveis

| Responsável            | Ação                                                                                                                                                 |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Fernando**           | Finalizar os ajustes das queries, incluindo `ano_calendario`, corrigindo os agrupamentos e removendo comentários desnecessários.                     |
| **Patrick**            | Materializar e atualizar as novas versões das tabelas em produção após o recebimento das queries ajustadas.                                          |
| **Gabi / Kamila**      | Validar a estrutura e a correção dos dados após as alterações.                                                                                       |
| **Gabi / Kamila** | Realizar os ajustes finos nas regras de negócio e validar se as métricas de frequência e faltas estão de acordo com as expectativas da área técnica. |
| **Fernanda**           | Comunicar o time de **Pequenos Cariocas** sobre as alterações na estrutura das tabelas e possíveis impactos em suas dependências.                    |

## Resultado esperado

Com os ajustes, as tabelas deverão apresentar **dados historicamente consistentes, segregados por aluno e ano**, evitando a inflação de aulas e faltas causada pela agregação entre diferentes anos e mantendo as regras de negócio de frequência alinhadas à realidade escolar.
