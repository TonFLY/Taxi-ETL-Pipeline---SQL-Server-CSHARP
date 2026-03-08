# TaxiPipeline ETL — Documentação Técnica Completa

> Documento detalhado explicando **cada arquivo** do projeto, o papel de cada um,
> e como os dados fluem desde o arquivo de entrada (CSV, Parquet ou API) até as tabelas finais no SQL Server.

---

## Sumário

1. [Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
2. [Estrutura de Diretórios](#2-estrutura-de-diretórios)
3. [Fluxo Completo dos Dados](#3-fluxo-completo-dos-dados)
4. [Arquivos SQL — Banco de Dados](#4-arquivos-sql--banco-de-dados)
   - 4.1 Criação do Banco
   - 4.2 Schemas
   - 4.3 Tabelas
   - 4.4 Índices
   - 4.5 Stored Procedures
5. [Arquivos C# — Aplicação](#5-arquivos-c--aplicação)
   - 5.1 Projeto Domain (Entidades, Enums, Interfaces)
   - 5.2 Projeto Application (Orquestração)
   - 5.3 Projeto Infrastructure (Banco, Arquivos, API, Logging)
   - 5.4 Projeto Console (Ponto de Entrada)
6. [Configuração](#6-configuração)
7. [Modos de Execução](#7-modos-de-execução)
8. [Ciclo de Vida de uma Execução (CSV)](#8-ciclo-de-vida-de-uma-execução-csv)
9. [Ciclo de Vida de uma Execução (API/Parquet)](#9-ciclo-de-vida-de-uma-execução-apiparquet)

---

## 1. Visão Geral da Arquitetura

O projeto segue uma arquitetura em **4 camadas no banco** e **4 projetos no C#**:

```
                        ┌─────────────────────┐
                        │   Console (Program)  │  ← Ponto de entrada (CLI)
                        └──────────┬──────────┘
                                   │
                        ┌──────────▼──────────┐
                        │    Application       │  ← Orquestração do pipeline
                        │  (PipelineOrchestrator)│
                        └──────────┬──────────┘
                                   │
    ┌──────────────────────────────┼────────────────────┐
    │                    │                    │          │
┌───▼─────────────┐  ┌──▼───────────┐  ┌────▼─────┐  ┌─▼──────────────┐
│  Infrastructure │  │Infrastructure│  │Infra     │  │ Infrastructure │
│   (Database)    │  │ (FileSystem) │  │(Logging) │  │   (Api)        │
└───┬─────────────┘  └──┬───────────┘  └──────────┘  └─┬──────────────┘
    │                   │                               │
    ▼                   ▼                               ▼
┌──────────┐    ┌─────────────┐                 ┌──────────────┐
│SQL Server│    │ CSV/Parquet │                 │ NYC TLC CDN  │
│(4 schemas)│   │   (local)   │                 │  (download)  │
└──────────┘    └─────────────┘                 └──────────────┘
```

**Fontes de dados suportadas:**

| Fonte | Formato | Como entra no pipeline |
|-------|---------|----------------------|
| Arquivo local CSV | `.csv` | Leitura direta via `CsvFileReaderService` |
| Arquivo local Parquet | `.parquet` | Leitura direta via `ParquetFileReaderService` |
| API NYC TLC | `.parquet` (CDN) | Download via `TaxiApiService` → leitura Parquet |

> O sistema detecta automaticamente o formato pelo extensão do arquivo
> usando o `FileReaderResolver` — sem necessidade de configuração.

**No banco de dados (4 schemas):**

| Schema    | Papel |
|-----------|-------|
| `ops`     | Controle operacional: batch, logs, erros, qualidade |
| `landing` | Dados brutos — exatamente como vieram do arquivo |
| `staging` | Dados limpos, validados, tipados e deduplicados |
| `core`    | Dados finais confiáveis — a "verdade" |

---

## 2. Estrutura de Diretórios

```
TaxiPipeline/
│
├── TaxiPipeline.slnx                          ← Arquivo de solução .NET
├── .gitignore                                  ← Regras de exclusão do Git
├── README.md                                   ← Visão geral do projeto
│
├── data/
│   ├── input/                                  ← Arquivos de entrada (CSV, Parquet, API downloads)
│   │   ├── yellow_tripdata_sample.csv          ← Arquivo CSV de teste (21 registros)
│   │   └── yellow_tripdata_2025-01.parquet     ← Exemplo: arquivo baixado da API
│   └── archive/                                ← Arquivos já processados
│
├── docs/
│   └── DOCUMENTACAO_TECNICA.md                 ← Este documento
│
├── sql/                                        ← Todos os scripts de banco
│   ├── 01_database/
│   │   └── 001_create_database.sql
│   ├── 02_schemas/
│   │   └── 001_create_schemas.sql
│   ├── 03_tables/
│   │   ├── 001_ops_tables.sql
│   │   ├── 002_landing_tables.sql
│   │   ├── 003_staging_tables.sql
│   │   └── 004_core_tables.sql
│   ├── 04_indexes/
│   │   └── 001_create_indexes.sql
│   └── 05_stored_procedures/
│       ├── ops/
│       │   ├── 001_usp_start_batch.sql
│       │   ├── 002_usp_finish_batch.sql
│       │   └── 003_usp_log_error.sql
│       ├── landing/
│       │   └── 001_usp_insert_yellow_trip_raw.sql
│       ├── staging/
│       │   ├── 001_usp_clean_yellow_trip_data.sql
│       │   ├── 002_usp_reject_invalid_yellow_trip_data.sql
│       │   └── 003_usp_deduplicate_yellow_trip_data.sql
│       └── core/
│           └── 001_usp_load_trip.sql
│
└── src/                                        ← Código C#
    ├── TaxiPipeline.Domain/                    ← Entidades, interfaces, enums
    │   ├── Entities/
    │   │   ├── AppSettings.cs
    │   │   ├── BatchContext.cs
    │   │   └── TripRecord.cs
    │   ├── Enums/
    │   │   ├── BatchStatus.cs
    │   │   └── PipelineStep.cs
    │   └── Interfaces/
    │       ├── IApiDataService.cs              ← NOVO: contrato para download da API
    │       ├── IBatchService.cs
    │       ├── IExecutionLogger.cs
    │       ├── IFileReaderService.cs
    │       ├── IPipelineOrchestrator.cs
    │       ├── IRawLoadService.cs
    │       └── IStoredProcedureExecutor.cs
    │
    ├── TaxiPipeline.Application/               ← Lógica de orquestração
    │   └── Orchestration/
    │       └── PipelineOrchestrator.cs
    │
    ├── TaxiPipeline.Infrastructure/            ← Implementações concretas
    │   ├── Api/                                ← NOVO: integração com API NYC TLC
    │   │   └── TaxiApiService.cs
    │   ├── Database/
    │   │   ├── SqlConnectionFactory.cs
    │   │   ├── BatchService.cs
    │   │   ├── RawLoadService.cs
    │   │   └── StoredProcedureExecutor.cs
    │   ├── FileSystem/
    │   │   ├── CsvFileReaderService.cs
    │   │   ├── ParquetFileReaderService.cs     ← NOVO: leitor de arquivos Parquet
    │   │   └── FileReaderResolver.cs           ← NOVO: resolve CSV vs Parquet automaticamente
    │   └── Logging/
    │       └── ExecutionLogger.cs
    │
    └── TaxiPipeline.Console/                   ← Aplicação executável
        ├── Program.cs
        ├── appsettings.json
        └── appsettings.Development.json
```

---

## 3. Fluxo Completo dos Dados

Aqui está o caminho que **cada linha de dado** percorre, do arquivo de entrada até a tabela final:

```
  ┌───────────────────┐     ┌────────────────────────────┐
  │ yellow_tripdata   │     │  NYC TLC API (CDN)         │
  │ _sample.csv       │     │  d37ci6vzurychx.cloudfront │
  └────────┬──────────┘     └─────────────┬──────────────┘
           │                              │
           │                              │  (0) TaxiApiService.DownloadTripDataAsync()
           │                              │      → Download streaming do Parquet
           │                              ▼
           │                    ┌─────────────────────────┐
           │                    │ yellow_tripdata_2025-01  │
           │                    │ .parquet (arquivo local) │
           │                    └─────────────┬───────────┘
           │                                  │
           │     ┌────────────────────────────┘
           │     │
           ▼     ▼
  ┌─────────────────────────────────────────────────┐
  │  FileReaderResolver                              │
  │  Detecta extensão → seleciona leitor:            │
  │    .csv     → CsvFileReaderService               │
  │    .parquet → ParquetFileReaderService            │
  └─────────────────────┬───────────────────────────┘
                        │
                        │  → List<TripRecord> (tudo como string)
                        ▼
  ┌─────────────────────────────┐
  │  landing.YellowTripRaw      │  ← Dados brutos (tudo NVARCHAR)
  │  via SqlBulkCopy            │     Preserva o dado original
  └──────────────┬──────────────┘
                 │
                 │  (2) Stored procedure: staging.usp_clean_yellow_trip_data
                 │      - TRY_CAST para converter tipos
                 │      - Calcula trip_duration_minutes
                 │      - Gera row_hash (SHA2_256)
                 ▼
  ┌─────────────────────────────┐
  │  staging.YellowTripClean    │  ← Dados tipados e limpos
  └──────────────┬──────────────┘
                 │
                 │  (3) Stored procedure: staging.usp_reject_invalid_yellow_trip_data
                 │      - Valida 11 regras de negócio
                 │      - Remove inválidos do Clean
                 │
          ┌──────┴──────┐
          │             │
          ▼             ▼
  ┌──────────────┐  ┌──────────────────────┐
  │  (válidos)   │  │ staging.YellowTrip   │  ← Registros rejeitados
  │  permanecem  │  │ Rejected             │     com motivo documentado
  │  no Clean    │  └──────────────────────┘
  └──────┬───────┘
         │
         │  (4) Stored procedure: staging.usp_deduplicate_yellow_trip_data
         │      - Marca duplicados dentro do batch
         │      - Marca duplicados contra core.Trip existente
         ▼
  ┌─────────────────────────────┐
  │  staging.YellowTripClean    │  ← is_duplicate = 0 nos válidos únicos
  │  (filtrado)                 │     is_duplicate = 1 nos repetidos
  └──────────────┬──────────────┘
                 │
                 │  (5) Stored procedure: core.usp_load_trip
                 │      - Insere apenas is_duplicate = 0
                 │      - Verifica novamente contra core.Trip
                 ▼
  ┌─────────────────────────────┐
  │  core.Trip                  │  ← TABELA FINAL — dados confiáveis
  │  (índice único no row_hash)│
  └─────────────────────────────┘
```

**Paralelamente**, tudo é rastreado no schema `ops`:

```
  ops.BatchControl      ← Cada execução = 1 registro com status e métricas
  ops.ExecutionLog      ← Cada etapa do pipeline = 1 registro com tempo e linhas
  ops.ExecutionError    ← Cada erro = 1 registro detalhado
  ops.DataQualityIssue  ← Cada problema de qualidade = 1 registro por regra
```

---

## 4. Arquivos SQL — Banco de Dados

### 4.1 `sql/01_database/001_create_database.sql`

**O que faz:** Cria o banco `TaxiPipelineDB` no SQL Server.

**Detalhes:**
- Verifica se o banco já existe antes de criar (idempotente)
- Usa `IF NOT EXISTS` para evitar erro em execuções repetidas
- Usa `CREATE DATABASE [TaxiPipelineDB]` sem especificar caminhos de arquivos, deixando o SQL Server usar os diretórios padrão da instância

**Quando executar:** Primeiro script a rodar, conectado no `master`.

---

### 4.2 `sql/02_schemas/001_create_schemas.sql`

**O que faz:** Cria os 4 schemas que organizam as tabelas por responsabilidade.

**Schemas criados:**

| Schema | Responsabilidade |
|--------|------------------|
| `landing` | Zona de pouso — dados brutos do arquivo |
| `staging` | Zona de preparação — dados limpos, validados |
| `core` | Zona final — dados confiáveis para uso |
| `ops` | Zona operacional — controle e auditoria |

**Detalhes:**
- Cada schema é criado com `IF NOT EXISTS` (idempotente)
- O dono de todos é `dbo`
- Essa separação em schemas é uma prática corporativa — facilita controle de permissões, organização e governança

---

### 4.3 Tabelas

#### `sql/03_tables/001_ops_tables.sql` — Tabelas Operacionais

Cria **4 tabelas** no schema `ops`:

**`ops.BatchControl`** — Coração do controle de execução
| Coluna | Tipo | O que guarda |
|--------|------|-------------|
| `batch_id` | BIGINT IDENTITY | Identificador único do lote |
| `batch_guid` | UNIQUEIDENTIFIER | GUID para rastreabilidade externa |
| `source_file_name` | NVARCHAR(500) | Nome do arquivo processado (CSV ou Parquet) |
| `batch_status` | VARCHAR(20) | STARTED, COMPLETED, FAILED ou REPROCESSING |
| `started_at` / `finished_at` | DATETIME2 | Início e fim da execução |
| `total_rows_*` | INT | Métricas: read, landed, cleaned, rejected, loaded |
| `error_message` | NVARCHAR(MAX) | Mensagem de erro, se houver |

→ **Cada vez que o pipeline roda, 1 registro é criado aqui.**

**`ops.ExecutionLog`** — Log de cada etapa
- Cada step do pipeline (Clean, Reject, Deduplicate, Load) gera um registro
- Guarda: nome do step, status, hora de início/fim, linhas afetadas

**`ops.ExecutionError`** — Erros detalhados
- Captura ERROR_NUMBER(), ERROR_MESSAGE(), ERROR_LINE() do T-SQL
- Também registra erros enviados pelo C#

**`ops.DataQualityIssue`** — Problemas de qualidade
- Registra quantos registros falharam em cada regra de validação
- Exemplo: "R004: 2 registros com trip_distance negativa"

---

#### `sql/03_tables/002_landing_tables.sql` — Tabelas de Landing

**`landing.ImportFile`** — Metadados do arquivo importado
- Nome, caminho, tamanho, hash do arquivo, quantidade de linhas
- Vinculado ao `batch_id`

**`landing.YellowTripRaw`** — Dados brutos da corrida de táxi

> **PONTO CRUCIAL:** Todas as colunas são `NVARCHAR(50)`.
> Isso é proposital — preserva o dado **exatamente** como veio da fonte (CSV ou Parquet),
> sem nenhuma conversão. Se o dado tem "abc" no campo `fare_amount`,
> esse "abc" é guardado aqui. A conversão só acontece no staging.
> Para dados Parquet, os valores tipados são convertidos para string antes da inserção,
> garantindo uniformidade na zona de landing.

| Coluna | Tipo | Observação |
|--------|------|-----------|
| `raw_id` | BIGINT IDENTITY | PK auto-incremento |
| `batch_id` | BIGINT | FK para ops.BatchControl |
| `source_line_number` | INT | Número da linha no arquivo fonte |
| `vendor_id` | NVARCHAR(50) | Tudo texto — sem conversão |
| `pickup_datetime` | NVARCHAR(50) | Data como string! |
| `fare_amount` | NVARCHAR(50) | Valor como string! |
| ... | NVARCHAR(50) | Todos os 19 campos como texto |
| `ingested_at` | DATETIME2 | Timestamp automático |

---

#### `sql/03_tables/003_staging_tables.sql` — Tabelas de Staging

**`staging.YellowTripClean`** — Dados limpos e tipados

> Aqui os dados já estão com os **tipos corretos**: DATETIME2 para datas,
> DECIMAL para valores, INT para IDs.

Colunas importantes adicionadas nesta camada:
| Coluna | O que é |
|--------|---------|
| `trip_duration_minutes` | Duração calculada = dropoff - pickup, em minutos |
| `row_hash` | VARBINARY(32) — Hash SHA2_256 dos campos de negócio |
| `is_duplicate` | BIT — 0 = único, 1 = duplicado |

**`staging.YellowTripRejected`** — Registros que falharam na validação

- Mantém os dados originais (NVARCHAR) para análise
- Adiciona `rejection_reason` (texto com todas as regras violadas)
- Adiciona `rejection_rule` (código da primeira regra violada: R001, R002, etc.)

---

#### `sql/03_tables/004_core_tables.sql` — Tabela Final

**`core.Trip`** — A tabela de verdade

- Recebe **apenas** registros que passaram em todas as validações E não são duplicados
- Tem os mesmos tipos do staging (DATETIME2, DECIMAL, INT)
- Inclui `row_hash` com **índice UNIQUE** — impede duplicatas mesmo em cenários de reprocessamento
- Campo `source_raw_id` permite rastrear de volta até o dado original no landing
- Colunas `trip_distance`, `fare_amount`, `total_amount` e `trip_duration_minutes` aceitam NULL para acomodar dados reais da NYC que eventualmente têm valores nulos

---

### 4.4 `sql/04_indexes/001_create_indexes.sql`

**O que faz:** Cria índices para performance em todas as tabelas.

Índices mais importantes:

| Tabela | Índice | Tipo | Para quê |
|--------|--------|------|---------|
| `core.Trip` | `IX_Trip_RowHash` | **UNIQUE** | Garante zero duplicatas |
| `core.Trip` | `IX_Trip_PickupDatetime` | NONCLUSTERED | Consultas por data |
| `staging.YellowTripClean` | `IX_YellowTripClean_RowHash` | NONCLUSTERED | Busca rápida na deduplicação |
| `ops.BatchControl` | `IX_BatchControl_Status` | NONCLUSTERED | Filtrar batches por status |
| Todas | `IX_*_BatchId` | NONCLUSTERED | Filtrar dados por lote |

Todos são criados com `IF NOT EXISTS` — podem rodar múltiplas vezes sem erro.

---

### 4.5 Stored Procedures

#### `ops/001_usp_start_batch.sql` — Abre um Batch

```
EXEC ops.usp_start_batch @source_file_name = 'arquivo.csv', @batch_id = @id OUTPUT
```

**O que faz:**
1. Insere 1 registro em `ops.BatchControl` com status = `STARTED`
2. Retorna o `batch_id` gerado (via `SCOPE_IDENTITY()`)
3. Registra em `ops.ExecutionLog` que o batch iniciou
4. Se der erro, registra em `ops.ExecutionError` e faz `THROW` (repropaga o erro)

**Por que importa:** Todo o pipeline depende desse `batch_id`. Cada dado inserido em qualquer tabela carrega esse ID, permitindo rastrear "de qual execução veio cada registro".

---

#### `ops/002_usp_finish_batch.sql` — Fecha um Batch

```
EXEC ops.usp_finish_batch @batch_id = 1, @batch_status = 'COMPLETED', @total_rows_loaded = 15, ...
```

**O que faz:**
1. Atualiza `ops.BatchControl` com:
   - Status final (COMPLETED ou FAILED)
   - `finished_at` = hora atual
   - Todas as métricas (rows read, landed, cleaned, rejected, loaded)
   - Mensagem de erro, se houver
2. Registra em `ops.ExecutionLog`

---

#### `ops/003_usp_log_error.sql` — Registra um Erro

**O que faz:**
- Insere em `ops.ExecutionError` com detalhes do erro
- Também insere em `ops.ExecutionLog` com status `FAILED`
- Chamada pelo C# quando acontece uma exceção em qualquer etapa

---

#### `landing/001_usp_insert_yellow_trip_raw.sql` — Insere Dados Brutos

**O que faz:**
- Recebe todos os 19 campos como `NVARCHAR(50)`
- Aplica `LTRIM(RTRIM(...))` para remover espaços
- Aplica `NULLIF(..., '')` para converter strings vazias em NULL
- Insere em `landing.YellowTripRaw`

> **Na prática**, essa procedure existe mais como referência.
> O C# usa **SqlBulkCopy** (muito mais rápido) para inserir os dados em massa.
> A procedure seria usada para cargas menores ou row-by-row.

---

#### `staging/001_usp_clean_yellow_trip_data.sql` — Limpa e Converte

**Esta é a procedure mais importante da transformação.**

**Entrada:** Dados brutos de `landing.YellowTripRaw` (tudo NVARCHAR)
**Saída:** Dados tipados em `staging.YellowTripClean`

**O que faz, passo a passo:**

1. **Deleta dados anteriores** do mesmo batch (idempotente — permite reprocessamento)

2. **Converte tipos** com `TRY_CAST`:
   - `vendor_id` → INT
   - `pickup_datetime` → DATETIME2(3)
   - `fare_amount` → DECIMAL(10,2)
   - etc.

   > `TRY_CAST` é seguro: se a conversão falhar, retorna NULL sem erro.
   > Exemplo: `TRY_CAST('abc' AS INT)` = NULL, sem quebrar a query.

3. **Padroniza texto**: `UPPER(LTRIM(RTRIM(...)))` no campo `store_and_fwd_flag`

4. **Calcula duração da corrida**:
   ```sql
   DATEDIFF(SECOND, pickup, dropoff) / 60.0  →  trip_duration_minutes
   ```

5. **Gera hash de deduplicação**:
   ```sql
   HASHBYTES('SHA2_256', CONCAT(vendor_id, '|', pickup, '|', dropoff, '|', ...))
   ```
   Combina os campos de negócio em uma string separada por `|` e gera um hash SHA-256.
   Dois registros idênticos terão o **mesmo hash**.

6. **Registra na tabela de log** o tempo e quantidade de linhas

**Filtro:** Só processa registros onde as datas podem ser convertidas (`TRY_CAST IS NOT NULL`).

---

#### `staging/002_usp_reject_invalid_yellow_trip_data.sql` — Valida e Rejeita

**O que faz:** Aplica 11 regras de validação e move os inválidos para a tabela de rejeição.

**Regras aplicadas:**

| Código | Regra | Exemplo de rejeição |
|--------|-------|-------------------|
| R001 | pickup_datetime não pode ser NULL | Data inválida no CSV |
| R002 | dropoff_datetime não pode ser NULL | Data inválida no CSV |
| R003 | dropoff deve ser posterior ao pickup | Horários invertidos |
| R004 | trip_distance >= 0 | Distância negativa: -3.5 |
| R005 | fare_amount >= 0 | Tarifa negativa |
| R006 | total_amount >= 0 | Total negativo |
| R007 | passenger_count entre 0 e 9 | Valor: 15 |
| R008 | Duração <= 720 min (12h) | Corrida de 2 dias |
| R009 | total_amount >= fare_amount | Total menor que tarifa |
| R010 | pickup_location_id entre 1 e 265 | Zona: 999 |
| R011 | dropoff_location_id entre 1 e 265 | Zona: 300 |

**Processo:**
1. Busca registros em `landing.YellowTripRaw` que violam **qualquer** regra
2. Insere em `staging.YellowTripRejected` com:
   - Todos os dados originais (NVARCHAR)
   - `rejection_reason` = todas as regras violadas (ex: "R003: dropoff not after pickup; R010: invalid pickup_location_id")
   - `rejection_rule` = código da primeira regra violada
3. **Remove** do `staging.YellowTripClean` os registros que foram rejeitados
4. Registra em `ops.DataQualityIssue` um resumo agrupado por regra

---

#### `staging/003_usp_deduplicate_yellow_trip_data.sql` — Remove Duplicatas

**O que faz:** Marca registros duplicados usando o `row_hash`.

**Dois tipos de deduplicação:**

1. **Dentro do batch** (within-batch):
   - Se o CSV tem 2 linhas iguais, só a primeira é mantida
   - Usa `ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY clean_id)` — mantém a de menor `clean_id`

2. **Entre batches** (cross-batch):
   - Se um registro já existe em `core.Trip`, marca como duplicado
   - Isso garante **idempotência**: se rodar o pipeline com o mesmo arquivo duas vezes, a segunda vez não insere nada

**Não deleta** — apenas marca `is_duplicate = 1`. O dado continua no staging para análise.

---

#### `core/001_usp_load_trip.sql` — Carga Final

**O que faz:** Insere os dados validados e únicos na tabela definitiva.

**Processo:**
1. **Deleta** registros do mesmo batch_id em `core.Trip` (permite reprocessamento)
2. Insere a partir de `staging.YellowTripClean` onde:
   - `is_duplicate = 0`
   - `row_hash` não existe em `core.Trip` (verificação final de segurança)
3. Registra no log a quantidade de linhas inseridas

---

## 5. Arquivos C# — Aplicação

### 5.1 Projeto Domain (`TaxiPipeline.Domain`)

> **Papel:** Define as "regras do jogo" — entidades, interfaces e enums.
> Não tem dependência de nenhum outro projeto. É a camada mais pura.

#### `Entities/TripRecord.cs`

```csharp
public class TripRecord
{
    public int SourceLineNumber { get; set; }
    public string? VendorId { get; set; }
    public string? PickupDatetime { get; set; }
    // ... todos os 19 campos como string
}
```

**O que é:** Representa uma linha de dados de uma corrida de táxi.
**Por que tudo é string?** Porque no momento da leitura do arquivo (CSV ou Parquet), não queremos rejeitar nada. O dado bruto vai para o banco como texto, e a conversão de tipos é feita pelo SQL Server no passo de limpeza. Para arquivos Parquet, valores tipados (DateTime, double) são convertidos para string pelo `ParquetFileReaderService`.

---

#### `Entities/BatchContext.cs`

```csharp
public class BatchContext
{
    public long BatchId { get; set; }
    public string SourceFileName { get; set; }
    public int TotalRowsRead { get; set; }
    public int TotalRowsLanded { get; set; }
    public int TotalRowsCleaned { get; set; }
    public int TotalRowsRejected { get; set; }
    public int TotalRowsLoaded { get; set; }

    public string GetSummary() { ... }
}
```

**O que é:** Objeto que acompanha a execução do pipeline inteiro. Vai acumulando as métricas de cada etapa e no final é enviado ao `ops.usp_finish_batch` com todos os números.

---

#### `Entities/AppSettings.cs`

```csharp
public class AppSettings
{
    public string ConnectionString { get; set; }
    public string InputDirectory { get; set; }
    public string ArchiveDirectory { get; set; }
    public string FilePattern { get; set; } = "*.csv";
    public int BulkCopyBatchSize { get; set; } = 5000;
    public int BulkCopyTimeoutSeconds { get; set; } = 120;
    public bool ArchiveAfterProcessing { get; set; } = true;
    public string CsvDelimiter { get; set; } = ",";

    // ── API Settings ───────────────────────────────
    public string ApiBaseUrl { get; set; }          // URL base da CDN da NYC TLC
    public string ApiFileNamePattern { get; set; }  // Padrão do nome do arquivo: {0}=ano, {1}=mês
    public int MaxRecordsFromApi { get; set; }       // Limite de registros (0 = sem limite)
    public string DownloadDirectory { get; set; }    // Pasta para downloads da API
}
```

**O que é:** Classe que espelha o `appsettings.json`. Os valores do JSON são carregados neste objeto pelo `ConfigurationBuilder` no `Program.cs`.

**Campos novos para a API:**

| Campo | Valor padrão | O que controla |
|-------|-------------|---------------|
| `ApiBaseUrl` | `https://d37ci6vzurychx.cloudfront.net/trip-data/` | CDN pública da NYC TLC |
| `ApiFileNamePattern` | `yellow_tripdata_{0}-{1:D2}.parquet` | Nome do arquivo. `{0}` = ano, `{1}` = mês |
| `MaxRecordsFromApi` | `0` (sem limite) | Limita registros lidos do Parquet (para testes) |
| `DownloadDirectory` | `""` (usa InputDirectory) | Pasta para salvar downloads |

---

#### `Enums/BatchStatus.cs`

Define os status possíveis: `Started`, `Completed`, `Failed`, `Reprocessing`.

#### `Enums/PipelineStep.cs`

Define as etapas: `StartBatch`, `ReadFile`, `InsertLanding`, `CleanData`, `RejectInvalid`, `Deduplicate`, `LoadCore`, `FinishBatch`.

---

#### Interfaces (`Interfaces/`)

> **Conceito:** Interfaces definem **contratos** — dizem "o que" deve ser feito,
> sem dizer "como". A implementação fica no projeto Infrastructure.
> Isso permite trocar implementações sem alterar a lógica do pipeline.

| Interface | Contrato | Quem implementa |
|-----------|---------|-----------------|
| `IFileReaderService` | `ReadFileAsync(filePath)` → Lista de TripRecord | `FileReaderResolver` → `CsvFileReaderService` ou `ParquetFileReaderService` |
| `IApiDataService` | `DownloadTripDataAsync(year, month)` → caminho local | `TaxiApiService` |
| `IBatchService` | `StartBatchAsync()`, `FinishBatchAsync()` | `BatchService` |
| `IRawLoadService` | `LoadRawDataAsync(batchId, records)` | `RawLoadService` |
| `IStoredProcedureExecutor` | `ExecuteWithRowCountAsync(procName, batchId)` | `StoredProcedureExecutor` |
| `IExecutionLogger` | `LogInfo()`, `LogError()`, `LogStepStart()` | `ExecutionLogger` |
| `IPipelineOrchestrator` | `RunAsync(file)`, `RunAllAsync()`, `RunFromApiAsync(year, month)` | `PipelineOrchestrator` |

---

### 5.2 Projeto Application (`TaxiPipeline.Application`)

> **Papel:** Contém a lógica de orquestração — a "partitura" do pipeline.
> Conhece apenas as interfaces do Domain. Não sabe como se conecta ao banco.

#### `Orchestration/PipelineOrchestrator.cs`

**Esta é a classe mais importante da aplicação.** Ela coordena o pipeline inteiro.

**Construtor — recebe todas as dependências:**
```csharp
public PipelineOrchestrator(
    IFileReaderService fileReader,      // Lê CSV ou Parquet (via FileReaderResolver)
    IBatchService batchService,          // Abre/fecha batch
    IRawLoadService rawLoadService,      // Insere no landing
    IStoredProcedureExecutor spExecutor, // Executa as procedures
    IExecutionLogger logger,             // Registra logs
    IApiDataService apiService,          // Baixa dados da API NYC TLC
    AppSettings settings)                // Configurações
```

**Método `RunAsync(filePath)` — executa o pipeline para 1 arquivo (CSV ou Parquet):**

```
Passo 1: _batchService.StartBatchAsync(fileName)
         → Chama ops.usp_start_batch → recebe batch_id

Passo 2: _fileReader.ReadFileAsync(filePath)
         → FileReaderResolver detecta extensão:
           .csv     → CsvFileReaderService.ReadFileAsync()
           .parquet → ParquetFileReaderService.ReadFileAsync()
         → retorna List<TripRecord> com todas as linhas

Passo 3: _rawLoadService.LoadRawDataAsync(batchId, records)
         → SqlBulkCopy → insere em landing.YellowTripRaw

Passo 4: _spExecutor.ExecuteWithRowCountAsync("staging.usp_clean_yellow_trip_data", ...)
         → Chama a procedure de limpeza → retorna qtd de linhas limpas

Passo 5: _spExecutor.ExecuteWithRowCountAsync("staging.usp_reject_invalid_...", ...)
         → Chama a procedure de rejeição → retorna qtd de rejeitados

Passo 6: _spExecutor.ExecuteWithRowCountAsync("staging.usp_deduplicate_...", ...)
         → Chama a procedure de deduplicação → retorna qtd de duplicados

Passo 7: _spExecutor.ExecuteWithRowCountAsync("core.usp_load_trip", ...)
         → Chama a procedure de carga final → retorna qtd de carregados

Passo 8: _batchService.FinishBatchAsync(context, success: true)
         → Fecha o batch com status COMPLETED e todas as métricas
```

**Tratamento de erro:** Se qualquer passo falhar, o `catch` registra o erro no banco (`LogErrorToDatabaseAsync`) e fecha o batch com status `FAILED`.

**Método `RunAllAsync()`:**
- Lista todos os arquivos no diretório de entrada que correspondem ao `FilePattern`
- Suporta múltiplos padrões separados por `;` (ex: `*.csv;*.parquet`)
- Chama `RunAsync()` para cada um, em sequência

**Método `RunFromApiAsync(year, month)` — NOVO:**
```
Passo 0: _apiService.DownloadTripDataAsync(year, month)
         → Baixa o Parquet da CDN da NYC TLC
         → Retorna caminho local do arquivo

Passos 1-8: RunAsync(localPath)
         → Executa o pipeline completo no arquivo baixado
```
- Se o arquivo já existe localmente, pula o download (cache)
- Mostra progresso do download a cada 2 segundos
- Se `MaxRecordsFromApi > 0`, limita a quantidade de registros lidos

---

### 5.3 Projeto Infrastructure (`TaxiPipeline.Infrastructure`)

> **Papel:** Implementações concretas — é aqui que o "como" acontece.
> Conecta-se ao SQL Server, lê arquivos do disco, grava logs.

#### `Database/SqlConnectionFactory.cs`

```csharp
public class SqlConnectionFactory
{
    private readonly string _connectionString;

    public async Task<SqlConnection> CreateOpenConnectionAsync(...)
    {
        var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}
```

**O que é:** Fábrica de conexões. Recebe a connection string uma vez e cria conexões sob demanda. Toda vez que uma classe precisa falar com o banco, pede uma conexão a esta fábrica.

---

#### `Database/BatchService.cs`

**Implementa `IBatchService`**

**`StartBatchAsync(fileName)`:**
1. Cria um `SqlCommand` do tipo `StoredProcedure`
2. Configura procedimento = `ops.usp_start_batch`
3. Adiciona parâmetro `@source_file_name` = nome do arquivo
4. Adiciona parâmetro `@batch_id` como OUTPUT
5. Executa `ExecuteNonQueryAsync()`
6. Lê `@batch_id` do parâmetro de saída e retorna

**`FinishBatchAsync(context, success)`:**
1. Chama `ops.usp_finish_batch` passando todas as métricas do `BatchContext`
2. Se `success = true` → status = `COMPLETED`
3. Se `success = false` → status = `FAILED` + mensagem de erro

---

#### `Database/RawLoadService.cs`

**Implementa `IRawLoadService`** — a peça de performance do sistema.

**`LoadRawDataAsync(batchId, records)`:**

1. **Cria um `DataTable`** espelhando a estrutura de `landing.YellowTripRaw`:
   - Cada `TripRecord` vira uma linha no DataTable
   - Cada campo passa por `NullIfEmpty()`: strings vazias viram `DBNull.Value`

2. **Usa `SqlBulkCopy`** para inserir em massa:
   ```csharp
   new SqlBulkCopy(connection)
   {
       DestinationTableName = "landing.YellowTripRaw",
       BatchSize = 5000,       // Insere 5000 linhas por vez
       BulkCopyTimeout = 120   // Timeout de 2 minutos
   }
   ```

3. **Mapeia colunas** explicitamente (`ColumnMappings.Add`)

> **Por que SqlBulkCopy?** É ordens de magnitude mais rápido que INSERT row-by-row.
> Para 1 milhão de linhas, a diferença pode ser de 1 minuto vs 30 minutos.

---

#### `Database/StoredProcedureExecutor.cs`

**Implementa `IStoredProcedureExecutor`**

```csharp
public async Task<int> ExecuteWithRowCountAsync(
    string procedureName,    // ex: "staging.usp_clean_yellow_trip_data"
    long batchId,           // ex: 2
    string outputParameterName, // ex: "@rows_cleaned"
    CancellationToken cancellationToken)
```

**O que faz:**
1. Abre conexão via `SqlConnectionFactory`
2. Cria `SqlCommand` com `CommandType = StoredProcedure`
3. Adiciona `@batch_id` como parâmetro de entrada
4. Adiciona o parâmetro de saída (nome varia: `@rows_cleaned`, `@rows_rejected`, etc.)
5. Executa `ExecuteNonQueryAsync()`
6. Retorna o valor do parâmetro de saída (quantidade de linhas afetadas)

**Timeout:** 300 segundos (5 minutos) para acomodar datasets grandes.

> **Essa classe é genérica** — funciona para qualquer procedure que receba `@batch_id`
> e retorne uma contagem. Por isso ela serve para Clean, Reject, Deduplicate e Load.

---

#### `FileSystem/CsvFileReaderService.cs`

**Implementa `IFileReaderService`** (para arquivos `.csv`)

**O que faz:** Lê um arquivo CSV e retorna uma lista de `TripRecord`.

**Mapeamento de colunas:**
O CSV público da NYC tem headers como `VendorID`, `tpep_pickup_datetime`, `PULocationID`.
A classe mantém um dicionário de mapeamento que aceita **múltiplos nomes** para cada campo:

```csharp
{ "VendorID", 0 },         // Nome do CSV real da NYC
{ "vendor_id", 0 },        // Nome alternativo
{ "tpep_pickup_datetime", 1 },  // Nome real
{ "pickup_datetime", 1 },       // Nome alternativo
{ "PULocationID", 7 },          // Nome real
{ "pickup_location_id", 7 },    // Nome alternativo
```

Isso permite que o sistema funcione com CSVs de diferentes origens sem alteração de código.

**Processo de leitura:**
1. Abre o arquivo com `StreamReader` (UTF-8)
2. Lê a primeira linha (header) e identifica qual coluna está em qual posição
3. Para cada linha seguinte:
   - Divide pelo delimitador (`,`)
   - Mapeia cada campo para a posição correta no `TripRecord`
   - Remove espaços e aspas dos valores
   - Converte strings vazias para `null`
4. Retorna a lista completa

---

#### `FileSystem/ParquetFileReaderService.cs` — NOVO

**Implementa `IFileReaderService`** (para arquivos `.parquet`)

**O que faz:** Lê arquivos Apache Parquet e retorna `List<TripRecord>`.

**Por que Parquet?**
A NYC TLC publica os dados de corridas de táxi em formato Parquet (não mais CSV) desde 2022.
Parquet é um formato colunar binário, **muito mais compacto** que CSV:
- CSV: ~200 MB para 1 mês → Parquet: ~55 MB (70% menor)
- Parquet já tem tipos nativos (int, double, timestamp) — mais confiável

**Biblioteca usada:** `Parquet.Net` (NuGet)

**Processo de leitura:**
1. Abre o arquivo Parquet via `ParquetReader.CreateAsync()`
2. Lê o schema (lista de colunas e tipos) do arquivo
3. Mapeia colunas pelo nome usando o mesmo dicionário de nomes que o CSV
4. Itera por **Row Groups** (blocos de dados do Parquet)
5. Para cada row group:
   - Lê todas as colunas de uma vez
   - Para cada linha, extrai os valores e converte para string
6. Respeita `MaxRecordsFromApi` — para de ler quando atinge o limite

**Conversão de tipos:**
```csharp
DateTimeOffset dto → dto.DateTime.ToString("yyyy-MM-dd HH:mm:ss")
double d           → d.ToString("G")
decimal dec        → dec.ToString("G")
```

> Por que converter para string? Porque o landing zone é todo NVARCHAR.
> O TRY_CAST no SQL Server fará a conversão tipada.

---

#### `FileSystem/FileReaderResolver.cs` — NOVO

**Implementa `IFileReaderService`** (composite — resolve qual leitor usar)

**O que faz:** Recebe um caminho de arquivo e, com base na extensão, direciona para o leitor correto:

```csharp
public Task<IReadOnlyList<TripRecord>> ReadFileAsync(string filePath, ...)
{
    var extension = Path.GetExtension(filePath);

    if (extension.Equals(".parquet", StringComparison.OrdinalIgnoreCase))
        return _parquetReader.ReadFileAsync(filePath, ...);

    return _csvReader.ReadFileAsync(filePath, ...);  // Default: CSV
}
```

> **Pattern usado:** Composite / Strategy — o `PipelineOrchestrator` não sabe
> se está processando CSV ou Parquet. Ele só conhece `IFileReaderService`.
> O `FileReaderResolver` faz a decisão transparentemente.

---

#### `Api/TaxiApiService.cs` — NOVO

**Implementa `IApiDataService`**

**O que faz:** Baixa arquivos Parquet da CDN pública da NYC TLC.

**URL do download:**
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ano}-{mes}.parquet
```
Exemplo: `yellow_tripdata_2025-01.parquet` (Janeiro 2025)

**Processo de download:**
1. **Valida** ano (2009–atual) e mês (1–12)
2. **Verifica cache** — se o arquivo já existe localmente, pula o download
3. **Download streaming** com buffer de 80 KB:
   - Não carrega o arquivo inteiro em memória
   - Mostra progresso em MB a cada 2 segundos
4. **Download atômico** — baixa para `.downloading` e renomeia ao final
   - Se o download falhar no meio, o arquivo parcial é deletado
5. Retorna o caminho local do arquivo baixado

**Dados disponíveis:** A NYC publica dados com ~2 meses de atraso. Ex: em março/2026, o dado mais recente disponível é de janeiro/2026.

**Tamanhos típicos:**
| Mês | Registros | Tamanho Parquet |
|-----|-----------|----------------|
| Jan 2025 | ~3.5 milhões | ~56 MB |
| Dez 2024 | ~3.2 milhões | ~52 MB |

---

#### `Logging/ExecutionLogger.cs`

**Implementa `IExecutionLogger`**

**Dual logging:**
1. **Console** — via `ILogger<ExecutionLogger>` do Microsoft.Extensions.Logging
   - Mostra no terminal em tempo real com timestamps
2. **Banco** — via `LogErrorToDatabaseAsync()`
   - Chama `ops.usp_log_error` para salvar erros na tabela `ops.ExecutionError`

Métodos de log:
- `LogInfo(msg)` → console apenas
- `LogWarning(msg)` → console apenas
- `LogError(msg, ex)` → console + stack trace
- `LogStepStart(name)` → console: "[STEP] CleanData - STARTED"
- `LogStepEnd(name, rows)` → console: "[STEP] CleanData - COMPLETED (21 rows)"
- `LogErrorToDatabaseAsync(batchId, step, msg)` → banco `ops.ExecutionError`

---

### 5.4 Projeto Console (`TaxiPipeline.Console`)

> **Papel:** Ponto de entrada da aplicação. Configura tudo e dispara o pipeline.

#### `Program.cs`

**O que faz, na ordem:**

1. **Carrega configuração:**
   ```csharp
   new ConfigurationBuilder()
       .AddJsonFile("appsettings.json")               // Configuração base
       .AddJsonFile("appsettings.Development.json")    // Override local (opcional)
       .AddEnvironmentVariables(prefix: "TAXIPIPELINE_") // Override por variável de ambiente
       .Build();
   ```
   → Os valores são carregados no objeto `AppSettings` via `configuration.GetSection("Pipeline").Bind(settings)`

2. **Valida configuração:** Verifica se `ConnectionString` e `InputDirectory` estão preenchidos

3. **Monta o container de DI (Dependency Injection):**
   ```
   IBatchService          → BatchService          (SqlConnectionFactory)
   IRawLoadService        → RawLoadService        (SqlConnectionFactory, AppSettings)
   IStoredProcedureExecutor → StoredProcedureExecutor (SqlConnectionFactory)
   IFileReaderService     → FileReaderResolver     (AppSettings) ← resolve CSV/Parquet
   IExecutionLogger       → ExecutionLogger       (ILogger, SqlConnectionFactory)
   IApiDataService        → TaxiApiService        (HttpClient, AppSettings) ← NOVO
   IPipelineOrchestrator  → PipelineOrchestrator  (todos os acima + AppSettings)
   HttpClient             → configurado com timeout de 30 min e User-Agent
   ```

   > **Injeção de Dependência:** O `PipelineOrchestrator` não sabe que está usando
   > `FileReaderResolver` — ele só conhece `IFileReaderService`. O resolver decide
   > automaticamente se usa CSV ou Parquet pela extensão do arquivo.

4. **Executa o pipeline (3 modos):**
   - `--api [year] [month]`: Baixa da API e processa → `orchestrator.RunFromApiAsync(year, month)`
   - Arquivo como argumento: `orchestrator.RunAsync(args[0])` (detecta CSV ou Parquet)
   - Sem argumentos: `orchestrator.RunAllAsync()` (processa todos no diretório de entrada)
   - `--help`: Mostra ajuda com exemplos de uso

5. **Ctrl+C:** Registra um handler de cancelamento que sinaliza o `CancellationToken`

6. **Exit code:**
   - `0` = sucesso
   - `1` = falha
   - `2` = cancelado pelo usuário

---

## 6. Configuração

#### `appsettings.json`

```json
{
  "Pipeline": {
    "ConnectionString": "Server=tonfly.cloud;Database=TaxiPipelineDB;User Id=sa;Password=...;TrustServerCertificate=True;",
    "InputDirectory": "..\\..\\..\\..\\..\\data\\input",
    "ArchiveDirectory": "..\\..\\..\\..\\..\\data\\archive",
    "DownloadDirectory": "..\\..\\..\\..\\..\\data\\input",
    "FilePattern": "*.csv",
    "BulkCopyBatchSize": 5000,
    "BulkCopyTimeoutSeconds": 120,
    "ArchiveAfterProcessing": true,
    "CsvDelimiter": ",",
    "ApiBaseUrl": "https://d37ci6vzurychx.cloudfront.net/trip-data/",
    "ApiFileNamePattern": "yellow_tripdata_{0}-{1:D2}.parquet",
    "MaxRecordsFromApi": 10000
  }
}
```

| Campo | O que controla |
|-------|---------------|
| `ConnectionString` | String de conexão com o SQL Server |
| `InputDirectory` | Pasta onde o programa procura arquivos CSV/Parquet |
| `ArchiveDirectory` | Pasta para onde move arquivos após processar |
| `DownloadDirectory` | Pasta para downloads da API (se vazio, usa InputDirectory) |
| `FilePattern` | Filtro de arquivos: `*.csv`, `*.parquet` ou `*.csv;*.parquet` |
| `BulkCopyBatchSize` | Quantas linhas o SqlBulkCopy envia por vez |
| `BulkCopyTimeoutSeconds` | Timeout do bulk insert |
| `ArchiveAfterProcessing` | Se deve mover o arquivo para archive após sucesso |
| `CsvDelimiter` | Caractere separador do CSV |
| `ApiBaseUrl` | URL base da CDN pública da NYC TLC |
| `ApiFileNamePattern` | Padrão do nome do arquivo. `{0}` = ano, `{1}` = mês |
| `MaxRecordsFromApi` | Limite de registros ao ler Parquet (0 = sem limite) |

#### `appsettings.Development.json`

Sobrescreve valores do `appsettings.json` em ambiente de desenvolvimento. Usa caminhos absolutos, desabilita arquivamento e limita a 1.000 registros da API para testes rápidos.

---

## 7. Modos de Execução

O pipeline suporta **3 modos** de execução via linha de comando:

```bash
# Modo 1: Processar todos os arquivos no diretório de entrada
dotnet run --project src/TaxiPipeline.Console

# Modo 2: Processar um arquivo específico (CSV ou Parquet)
dotnet run --project src/TaxiPipeline.Console -- data/input/arquivo.csv
dotnet run --project src/TaxiPipeline.Console -- data/input/arquivo.parquet

# Modo 3: Baixar da API NYC TLC e processar
dotnet run --project src/TaxiPipeline.Console -- --api              # último mês disponível
dotnet run --project src/TaxiPipeline.Console -- --api 2025 1       # janeiro 2025
dotnet run --project src/TaxiPipeline.Console -- --api 2024 6       # junho 2024
# Bem facil adicionar um script para processar a quantidade dados necessarios.


# Ajuda
dotnet run --project src/TaxiPipeline.Console -- --help
```

**Detecção automática de formato:**
| Extensão do arquivo | Leitor usado |
|----|-----|
| `.csv` | `CsvFileReaderService` — leitura por StreamReader, split por delimitador |
| `.parquet` | `ParquetFileReaderService` — leitura colunar via Parquet.Net |
| Qualquer outra | `CsvFileReaderService` (fallback) |

---

## 8. Ciclo de Vida de uma Execução (CSV)

Exemplo real da execução com o arquivo `yellow_tripdata_sample.csv` (21 linhas):

```
[11:22:03] Pipeline starting for file: yellow_tripdata_sample.csv

STEP 1 — StartBatch
   C#: BatchService.StartBatchAsync("yellow_tripdata_sample.csv")
   SQL: EXEC ops.usp_start_batch → batch_id = 2
   Resultado: 1 registro em ops.BatchControl (status = STARTED)

STEP 2 — ReadFile
   C#: FileReaderResolver → extensão .csv → CsvFileReaderService.ReadFileAsync()
   Resultado: 21 objetos TripRecord em memória

STEP 3 — InsertLanding
   C#: RawLoadService.LoadRawDataAsync(batchId=2, 21 records)
   SQL: SqlBulkCopy → landing.YellowTripRaw (21 linhas inseridas como texto)

STEP 4 — CleanData
   C#: StoredProcedureExecutor → EXEC staging.usp_clean_yellow_trip_data @batch_id=2
   SQL: TRY_CAST em todas as colunas, calcula duração e hash
   Resultado: 21 linhas em staging.YellowTripClean (tipos corretos)

STEP 5 — RejectInvalid
   C#: StoredProcedureExecutor → EXEC staging.usp_reject_invalid_yellow_trip_data @batch_id=2
   SQL: Aplica 11 regras de validação
   Resultado: 5 linhas movidas para staging.YellowTripRejected
             16 linhas permanecem no YellowTripClean
   Rejeitados:
     - Linha 16: R003 (dropoff antes do pickup)
     - Linha 17: R004 (distância negativa)
     - Linha 18: R007 (15 passageiros)
     - Linha 19: R010/R011 (localização 999/300)
     - Linha 20: R005/R006 (valores negativos)

STEP 6 — Deduplicate
   C#: StoredProcedureExecutor → EXEC staging.usp_deduplicate_yellow_trip_data @batch_id=2
   SQL: Compara row_hash dentro do batch e contra core.Trip
   Resultado: 1 duplicata marcada (linha 21 = cópia da linha 1)

STEP 7 — LoadCore
   C#: StoredProcedureExecutor → EXEC core.usp_load_trip @batch_id=2
   SQL: INSERT INTO core.Trip (WHERE is_duplicate = 0 AND hash not in core)
   Resultado: 15 linhas inseridas na tabela final

STEP 8 — FinishBatch
   C#: BatchService.FinishBatchAsync(context, success=true)
   SQL: EXEC ops.usp_finish_batch → status = COMPLETED
   Resultado: ops.BatchControl atualizado com todas as métricas

[11:22:05] Pipeline COMPLETED successfully.
           Batch 2 | Read: 21 | Landed: 21 | Cleaned: 21 | Rejected: 5 | Loaded: 15
```

**Resumo final (CSV):**

| Onde | Quantidade | Estado |
|------|-----------|--------|
| landing.YellowTripRaw | 21 | Dados brutos preservados |
| staging.YellowTripClean | 16 | 15 válidos + 1 duplicado |
| staging.YellowTripRejected | 5 | Rejeitados com motivo |
| **core.Trip** | **15** | **Dados confiáveis finais** |
| ops.BatchControl | 1 | Batch COMPLETED |
| ops.ExecutionLog | ~10 | 1 por etapa |
| ops.DataQualityIssue | ~6 | 1 por regra violada + 1 dedup |

---

## 9. Ciclo de Vida de uma Execução (API/Parquet)

Exemplo real da execução com `--api 2025 1` (Janeiro 2025, limitado a 1.000 registros):

```
[11:44:16] API MODE - Downloading data for 2025-01

STEP 0 — DownloadFromApi
   C#: TaxiApiService.DownloadTripDataAsync(2025, 1)
   URL: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
   Download: 56.4 MB (streaming com progresso)
   Salvo em: data/input/yellow_tripdata_2025-01.parquet
   (Se já existir localmente, pula o download)

STEP 1 — StartBatch
   C#: BatchService.StartBatchAsync("yellow_tripdata_2025-01.parquet")
   SQL: EXEC ops.usp_start_batch → batch_id = 4

STEP 2 — ReadFile
   C#: FileReaderResolver → extensão .parquet → ParquetFileReaderService.ReadFileAsync()
   Parquet: Lê Row Groups coluna a coluna
   MaxRecordsFromApi: 1.000 (para no limite)
   Resultado: 1.000 objetos TripRecord em memória

STEP 3 — InsertLanding
   C#: SqlBulkCopy → landing.YellowTripRaw (1.000 linhas)

STEP 4 — CleanData
   SQL: TRY_CAST + hash → 1.000 linhas em staging.YellowTripClean

STEP 5 — RejectInvalid
   SQL: 11 regras de validação → 3 rejeitados

STEP 6 — Deduplicate
   SQL: Comparação por row_hash → 0 duplicados

STEP 7 — LoadCore
   SQL: INSERT INTO core.Trip → 997 registros carregados

STEP 8 — FinishBatch
   SQL: status = COMPLETED

[11:44:18] Pipeline COMPLETED successfully.
           Batch 4 | Read: 1000 | Landed: 1000 | Cleaned: 1000 | Rejected: 3 | Loaded: 997
```

**Resumo final (API - 1.000 registros de Jan/2025):**

| Onde | Quantidade | Estado |
|------|-----------|--------|
| landing.YellowTripRaw | 1.000 | Dados reais da NYC convertidos para texto |
| staging.YellowTripClean | 997 | Dados tipados, limpos, únicos |
| staging.YellowTripRejected | 3 | Rejeitados por regras de qualidade |
| **core.Trip** | **997** | **Dados confiáveis finais** |
| ops.BatchControl | 1 | Batch COMPLETED |

> **Para processar TODOS os registros** de um mês (~3.5 milhões),
> basta alterar `MaxRecordsFromApi` para `0` no `appsettings.json`.

---

*Documento atualizado em 08/03/2026 para o projeto TaxiPipeline ETL v2.0 (suporte a API + Parquet).*
