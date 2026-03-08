# TaxiPipeline ETL

Pipeline ETL profissional para ingestão, limpeza, validação, deduplicação e carga de dados públicos de corridas de táxi amarelo de Nova York (*NYC Yellow Taxi Trip Data*), utilizando **SQL Server** como banco de dados e **C#/.NET 9** como orquestrador.

Suporta três modos de ingestão: **arquivos CSV locais**, **arquivos Parquet locais** e **download direto da API pública da NYC TLC**.

---

## Sumário

- [Objetivo](#objetivo)
- [Arquitetura](#arquitetura)
- [Fluxo do Pipeline](#fluxo-do-pipeline)
- [Tecnologias](#tecnologias)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Configuração](#instalação-e-configuração)
- [Modos de Execução](#modos-de-execução)
- [Schemas do Banco de Dados](#schemas-do-banco-de-dados)
- [Tabelas](#tabelas)
- [Stored Procedures](#stored-procedures)
- [Regras de Validação](#regras-de-validação)
- [Deduplicação](#deduplicação)
- [Projetos C#](#projetos-c)
- [Configurações](#configurações)
- [Segurança — Proteção de Credenciais](#segurança--proteção-de-credenciais)
- [Consultas de Verificação](#consultas-de-verificação)
- [Decisões Técnicas](#decisões-técnicas)
- [Melhorias Futuras](#melhorias-futuras)
- [Fonte de Dados](#fonte-de-dados)

---

## Objetivo

Demonstrar domínio de engenharia de dados com foco em:

- Arquitetura de dados em camadas (Landing → Staging → Core)
- Stored procedures T-SQL com conversão segura, validação e deduplicação
- Código C# orientado a objetos com separação de responsabilidades (Clean Architecture)
- Controle de lote, rastreabilidade e tratamento de erros
- Carga idempotente e deduplicação por hash SHA2_256
- Qualidade de dados com 11 regras de validação e rejeição documentada
- Ingestão multi-formato (CSV, Parquet) e download direto de API pública

---

<h1 align="center">TaxiPipeline ETL</h1>

<p align="center">
  Arquitetura de ingestão, leitura e processamento de dados em camadas
</p>

<br>

<table align="center">
  <tr>
    <td align="center" colspan="3"><b>Fontes de Dados</b></td>
  </tr>
  <tr>
    <td align="center" width="220">
      <img src="https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/svg/1f4c4.svg" width="28" /><br>
      <b>CSV Local</b><br>
      <sub>Arquivos planos</sub>
    </td>
    <td align="center" width="220">
      <img src="https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/svg/1f4e6.svg" width="28" /><br>
      <b>Parquet Local</b><br>
      <sub>Arquivos colunares</sub>
    </td>
    <td align="center" width="220">
      <img src="https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/svg/1f310.svg" width="28" /><br>
      <b>API NYC TLC</b><br>
      <sub>Fonte externa via CDN</sub>
    </td>
  </tr>
</table>

<p align="center">
  ↓
</p>

<table align="center">
  <tr>
    <td align="center" width="680">
      <b>TaxiApiService</b><br>
      <sub>Download em streaming e aquisição de dados externos</sub>
    </td>
  </tr>
</table>

<p align="center">
  ↓
</p>

<table align="center">
  <tr>
    <td align="center" width="680">
      <b>FileReaderResolver</b><br>
      <sub>Detecção automática do tipo de arquivo e roteamento do leitor</sub>
    </td>
  </tr>
</table>

<br>

<table align="center">
  <tr>
    <td align="center" width="340">
      <b>.csv</b><br>
      <sub>CsvFileReaderService</sub>
    </td>
    <td align="center" width="340">
      <b>.parquet</b><br>
      <sub>ParquetFileReaderService</sub>
    </td>
  </tr>
</table>

<p align="center">
  ↓
</p>

<table align="center">
  <tr>
    <td align="center" colspan="4"><b>Camadas de Processamento de Dados</b></td>
  </tr>
  <tr>
    <td align="center" width="170">
      <b>Landing</b><br>
      <sub>Ingestão de dados brutos</sub>
    </td>
    <td align="center" width="170">
      <b>Staging</b><br>
      <sub>Limpeza e validação</sub>
    </td>
    <td align="center" width="170">
      <b>Core</b><br>
      <sub>Dados confiáveis e curados</sub>
    </td>
    <td align="center" width="170">
      <b>Ops</b><br>
      <sub>Consumo operacional</sub>
    </td>
  </tr>
</table>

</br>
</br>
</br>
</br>

---

<h2 align="center">Arquitetura em Camadas — C#</h2>

<p align="center">
  Estrutura da aplicação com separação de responsabilidades entre entrada, orquestração, infraestrutura e domínio
</p>

<br>

<table align="center">
  <tr>
    <td align="center" width="720">
      <b>Console</b><br>
      <sub><code>Program.cs</code> · Ponto de entrada da aplicação · Configuração de CLI e DI</sub>
    </td>
  </tr>
</table>

<p align="center"><b>↓</b></p>

<table align="center">
  <tr>
    <td align="center" width="720">
      <b>Application</b><br>
      <sub><code>PipelineOrchestrator</code> · Orquestração do pipeline · Coordenação das 8 etapas</sub>
    </td>
  </tr>
</table>

<p align="center"><b>↓</b></p>

<table align="center">
  <tr>
    <td align="center" colspan="4"><b>Infrastructure</b></td>
  </tr>
  <tr>
    <td align="center" width="180">
      <b>Database</b><br>
      <sub>Acesso a dados e persistência</sub>
    </td>
    <td align="center" width="180">
      <b>FileSystem</b><br>
      <sub>Leitura, escrita e gerenciamento de arquivos</sub>
    </td>
    <td align="center" width="180">
      <b>API</b><br>
      <sub>Integrações externas</sub>
    </td>
    <td align="center" width="180">
      <b>Logging</b><br>
      <sub>Observabilidade, auditoria e diagnósticos</sub>
    </td>
  </tr>
</table>

<p align="center"><b>↓</b></p>

<table align="center">
  <tr>
    <td align="center" width="720">
      <b>Domain</b><br>
      <sub>Entidades · Interfaces · Enums · Regras e contratos centrais do sistema</sub>
    </td>
  </tr>
</table>

---

## Fluxo do Pipeline

Cada execução segue **8 etapas sequenciais**:

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Etapa 1 │ StartBatch       │ Abre registro no ops.BatchControl         │
│  Etapa 2 │ ReadFile         │ Lê CSV ou Parquet → List<TripRecord>      │
│  Etapa 3 │ InsertLanding    │ SqlBulkCopy → landing.YellowTripRaw       │
│  Etapa 4 │ CleanData        │ TRY_CAST + hash → staging.YellowTripClean │
│  Etapa 5 │ RejectInvalid    │ 11 regras → staging.YellowTripRejected    │
│  Etapa 6 │ Deduplicate      │ SHA2_256 → marca is_duplicate = 1         │
│  Etapa 7 │ LoadCore         │ Insere únicos válidos → core.Trip          │
│  Etapa 8 │ FinishBatch      │ Atualiza métricas e status do batch       │
└──────────────────────────────────────────────────────────────────────────┘
```

</br>
</br>

---

<h2 align="center">Fluxo de Processamento de Dados</h2>

<p align="center">
Pipeline de transformação desde a ingestão até os dados confiáveis
</p>

<table align="center">
<tr>
<td align="center" width="700">

<b>Arquivo de Entrada</b><br>
<sub>CSV · Parquet · API</sub>

</td>
</tr>
</table>

<p align="center">⬇</p>

<table align="center">
<tr>
<td align="center" width="700">

<b>landing.YellowTripRaw</b><br>
<sub>Dados brutos preservados</sub><br>
<sub>Todas as colunas armazenadas como <code>NVARCHAR</code></sub>

</td>
</tr>
</table>

<p align="center">⬇</p>

<table align="center">
<tr>
<td align="center" width="700">

<b>staging.YellowTripClean</b><br>
<sub>Tipagem correta das colunas</sub><br>
<sub>Hash gerado para deduplicação</sub><br>
<sub>Duração da corrida calculada</sub>

</td>
</tr>
</table>

<p align="center">⬇</p>

<table align="center">
<tr>

<td align="center" width="350">

<b>Registros Válidos</b><br>
<sub>Dados que passaram nas validações</sub>

</td>

<td align="center" width="350">

<b>staging.YellowTripRejected</b><br>
<sub>Registros inválidos</sub><br>
<sub>Motivo da rejeição armazenado</sub>

</td>

</tr>
</table>

<p align="center">⬇</p>

<table align="center">
<tr>
<td align="center" width="700">

<b>Deduplicação</b><br>
<sub>Campo <code>is_duplicate</code> = 0 ou 1</sub>

</td>
</tr>
</table>

<p align="center">⬇</p>

<table align="center">
<tr>
<td align="center" width="700">

<b>core.Trip</b><br>
<sub>Tabela final confiável</sub><br>
<sub>Índice <code>UNIQUE</code> baseado no hash</sub>

</td>
</tr>
</table>

---

## Tecnologias

| Componente        | Tecnologia                                |
|-------------------|-------------------------------------------|
| Banco de dados    | SQL Server 2019+                          |
| Linguagem         | C# / .NET 9                               |
| Acesso ao banco   | Microsoft.Data.SqlClient 5.2.2            |
| Leitura Parquet   | Parquet.Net                               |
| Injeção de dep.   | Microsoft.Extensions.DependencyInjection  |
| Configuração      | Microsoft.Extensions.Configuration        |
| Logging           | Microsoft.Extensions.Logging              |
| Bulk insert       | SqlBulkCopy                               |
| Download API      | HttpClient (streaming)                    |
| Deduplicação      | HASHBYTES SHA2_256                        |

---

## Estrutura do Projeto

```
TaxiPipeline/
├── TaxiPipeline.slnx                              Arquivo de solução .NET
├── .gitignore                                      Regras de exclusão do Git
├── README.md                                       Este documento
│
├── data/
│   ├── input/                                      Arquivos de entrada (CSV, Parquet, downloads)
│   └── archive/                                    Arquivos processados (movidos automaticamente)
│
├── docs/
│   └── DOCUMENTACAO_TECNICA.md                     Documentação técnica detalhada arquivo por arquivo
│
├── sql/
│   ├── 01_database/
│   │   └── 001_create_database.sql                 Criação do banco TaxiPipelineDB
│   ├── 02_schemas/
│   │   └── 001_create_schemas.sql                  4 schemas: landing, staging, core, ops
│   ├── 03_tables/
│   │   ├── 001_ops_tables.sql                      BatchControl, ExecutionLog, ExecutionError, DataQualityIssue
│   │   ├── 002_landing_tables.sql                  ImportFile, YellowTripRaw
│   │   ├── 003_staging_tables.sql                  YellowTripClean, YellowTripRejected
│   │   └── 004_core_tables.sql                     Trip (tabela final)
│   ├── 04_indexes/
│   │   └── 001_create_indexes.sql                  Índices para performance (incluindo UNIQUE no hash)
│   └── 05_stored_procedures/
│       ├── ops/
│       │   ├── 001_usp_start_batch.sql             Abre batch
│       │   ├── 002_usp_finish_batch.sql            Fecha batch com métricas
│       │   └── 003_usp_log_error.sql               Registra erros
│       ├── landing/
│       │   └── 001_usp_insert_yellow_trip_raw.sql  Insert individual (backup ao BulkCopy)
│       ├── staging/
│       │   ├── 001_usp_clean_yellow_trip_data.sql  Limpeza + conversão de tipos + hash
│       │   ├── 002_usp_reject_invalid_yellow_trip_data.sql  Validação de 11 regras
│       │   └── 003_usp_deduplicate_yellow_trip_data.sql     Deduplicação intra e cross-batch
│       └── core/
│           └── 001_usp_load_trip.sql               Carga final (apenas únicos válidos)
│
└── src/
    ├── TaxiPipeline.Domain/                        Entidades, interfaces, enums
    │   ├── Entities/
    │   │   ├── AppSettings.cs                      Configurações da aplicação
    │   │   ├── BatchContext.cs                     Contexto de execução com métricas
    │   │   └── TripRecord.cs                       Entidade de corrida (19 campos como string)
    │   ├── Enums/
    │   │   ├── BatchStatus.cs                      Started, Completed, Failed, Reprocessing
    │   │   └── PipelineStep.cs                     8 etapas do pipeline
    │   └── Interfaces/
    │       ├── IApiDataService.cs                  Contrato para download da API
    │       ├── IBatchService.cs                    Contrato para controle de batch
    │       ├── IExecutionLogger.cs                 Contrato para logging
    │       ├── IFileReaderService.cs               Contrato para leitura de arquivos
    │       ├── IPipelineOrchestrator.cs            Contrato para orquestração
    │       ├── IRawLoadService.cs                  Contrato para carga no landing
    │       └── IStoredProcedureExecutor.cs         Contrato para execução de SPs
    │
    ├── TaxiPipeline.Application/                   Lógica de orquestração
    │   └── Orchestration/
    │       └── PipelineOrchestrator.cs             Orquestra as 8 etapas com error handling
    │
    ├── TaxiPipeline.Infrastructure/                Implementações concretas
    │   ├── Api/
    │   │   └── TaxiApiService.cs                   Download streaming da API NYC TLC
    │   ├── Database/
    │   │   ├── SqlConnectionFactory.cs             Factory de conexões SQL Server
    │   │   ├── BatchService.cs                     Implementa IBatchService
    │   │   ├── RawLoadService.cs                   SqlBulkCopy para landing
    │   │   └── StoredProcedureExecutor.cs          Execução genérica de SPs com output
    │   ├── FileSystem/
    │   │   ├── CsvFileReaderService.cs             Leitor de CSV com mapeamento flexível
    │   │   ├── ParquetFileReaderService.cs         Leitor de Parquet via Parquet.Net
    │   │   └── FileReaderResolver.cs               Resolve CSV vs Parquet pela extensão
    │   └── Logging/
    │       └── ExecutionLogger.cs                  Logging duplo: console + banco (ops)
    │
    └── TaxiPipeline.Console/                       Aplicação executável
        ├── Program.cs                              Entry point com DI, CLI --api/--help
        ├── appsettings.json                        Configurações base (sem credenciais)
        └── appsettings.Development.json            Configurações locais (gitignored)
```

---

## Pré-requisitos

| Requisito | Versão mínima |
|-----------|---------------|
| SQL Server | 2019 (Express, Developer ou Standard) |
| .NET SDK | 9.0 |
| Permissões | `sysadmin` ou `dbcreator` no SQL Server |

---

## Instalação e Configuração

### 1. Clonar o Repositório

```bash
git clone https://github.com/TonFLY/Taxi-ETL-Pipeline---SQL-Server-CSHARP.git
cd Taxi-ETL-Pipeline---SQL-Server-CSHARP/TaxiPipeline
```

### 2. Criar o Banco de Dados

Execute os scripts SQL **na ordem numérica** no SQL Server Management Studio, Azure Data Studio ou `sqlcmd`:

```
sql/01_database/001_create_database.sql
sql/02_schemas/001_create_schemas.sql
sql/03_tables/001_ops_tables.sql
sql/03_tables/002_landing_tables.sql
sql/03_tables/003_staging_tables.sql
sql/03_tables/004_core_tables.sql
sql/04_indexes/001_create_indexes.sql
sql/05_stored_procedures/ops/001_usp_start_batch.sql
sql/05_stored_procedures/ops/002_usp_finish_batch.sql
sql/05_stored_procedures/ops/003_usp_log_error.sql
sql/05_stored_procedures/landing/001_usp_insert_yellow_trip_raw.sql
sql/05_stored_procedures/staging/001_usp_clean_yellow_trip_data.sql
sql/05_stored_procedures/staging/002_usp_reject_invalid_yellow_trip_data.sql
sql/05_stored_procedures/staging/003_usp_deduplicate_yellow_trip_data.sql
sql/05_stored_procedures/core/001_usp_load_trip.sql
```

### 3. Configurar a Connection String

Crie o arquivo `src/TaxiPipeline.Console/appsettings.Development.json`
```json
{
  "Pipeline": {
    "ConnectionString": "Server=SEU_SERVIDOR;Database=TaxiPipelineDB;User Id=SEU_USUARIO;Password=SUA_SENHA;TrustServerCertificate=True;",
    "InputDirectory": "C:\\caminho\\para\\data\\input",
    "ArchiveDirectory": "C:\\caminho\\para\\data\\archive",
    "DownloadDirectory": "C:\\caminho\\para\\data\\input",
    "ArchiveAfterProcessing": false,
    "MaxRecordsFromApi": 1000
  }
}
```

> **Importante:** Nunca coloque credenciais reais no `appsettings.json`. Use `appsettings.Development.json` ou `appsettings.Local.json`, que são ignorados pelo Git.

### 4. Restaurar Dependências e Compilar

```powershell
dotnet restore src/TaxiPipeline.Console/TaxiPipeline.Console.csproj
dotnet build src/TaxiPipeline.Console/TaxiPipeline.Console.csproj
```

---

## Modos de Execução

### Processar todos os arquivos do diretório de entrada

```powershell
dotnet run --project src/TaxiPipeline.Console
```

Processa todos os arquivos que correspondam ao padrão `FilePattern` (padrão: `*.csv`) no `InputDirectory`.

### Processar um arquivo específico

```powershell
dotnet run --project src/TaxiPipeline.Console -- "C:\caminho\para\arquivo.csv"
dotnet run --project src/TaxiPipeline.Console -- "C:\caminho\para\arquivo.parquet"
```

O sistema detecta automaticamente o formato pela extensão do arquivo.

### Download e processamento da API NYC TLC

```powershell
# Baixar o mês mais recente disponível (padrão: 2 meses atrás)
dotnet run --project src/TaxiPipeline.Console -- --api

# Baixar um mês específico
dotnet run --project src/TaxiPipeline.Console -- --api 2025 1

# Baixar junho de 2024
dotnet run --project src/TaxiPipeline.Console -- --api 2024 6
```

O pipeline faz download streaming do arquivo Parquet (~50-80 MB) do CDN da NYC TLC, salva localmente e processa automaticamente.

> A NYC publica dados com ~2 meses de atraso. Dados de janeiro 2025 ficam disponíveis por volta de março 2025.

### Exibir ajuda

```powershell
dotnet run --project src/TaxiPipeline.Console -- --help
```

---

## Schemas do Banco de Dados

| Schema    | Responsabilidade | Tabelas |
|-----------|------------------|---------|
| `ops`     | Controle operacional: batch, logs, erros, métricas de qualidade | 4 tabelas |
| `landing` | Dados brutos — exatamente como vieram do arquivo (tudo NVARCHAR) | 2 tabelas |
| `staging` | Dados limpos, tipados, validados e deduplicados | 2 tabelas |
| `core`    | Dados finais confiáveis — single source of truth | 1 tabela |

---

## Tabelas

### Schema `ops` (Operacional)

| Tabela | Descrição |
|--------|-----------|
| `ops.BatchControl` | Cada execução = 1 registro. Guarda status (STARTED/COMPLETED/FAILED), métricas (rows read, landed, cleaned, rejected, loaded), timestamps |
| `ops.ExecutionLog` | Cada etapa do pipeline = 1 registro com nome, status, hora início/fim, linhas afetadas |
| `ops.ExecutionError` | Captura `ERROR_NUMBER()`, `ERROR_MESSAGE()`, `ERROR_LINE()` do T-SQL + erros do C# |
| `ops.DataQualityIssue` | Agrega problemas de qualidade por regra (ex: "R004: 3 registros com trip_distance negativa") |

### Schema `landing` (Ingestão)

| Tabela | Descrição |
|--------|-----------|
| `landing.ImportFile` | Metadados do arquivo importado (nome, tamanho, hash) |
| `landing.YellowTripRaw` | **Todas as colunas são NVARCHAR(50)** — preserva o dado original sem conversão. 19 campos de corrida + batch_id + source_line_number + ingested_at |

### Schema `staging` (Preparação)

| Tabela | Descrição |
|--------|-----------|
| `staging.YellowTripClean` | Dados com tipos corretos (DATETIME2, DECIMAL, INT). Inclui `trip_duration_minutes` (calculado), `row_hash` (SHA2_256) e `is_duplicate` |
| `staging.YellowTripRejected` | Registros que falharam na validação. Mantém dados originais + `rejection_reason` (texto) + `rejection_rule` (código R001-R011) |

### Schema `core` (Final)

| Tabela | Descrição |
|--------|-----------|
| `core.Trip` | Single source of truth. Só recebe registros válidos e não duplicados. Índice UNIQUE no `row_hash` garante zero duplicatas mesmo em reprocessamento |

---

## Stored Procedures

| Procedure | Schema | O que faz |
|-----------|--------|-----------|
| `usp_start_batch` | ops | Cria registro no BatchControl, retorna `@batch_id` |
| `usp_finish_batch` | ops | Atualiza status, métricas e timestamps do batch |
| `usp_log_error` | ops | Registra erros no ExecutionError e ExecutionLog |
| `usp_insert_yellow_trip_raw` | landing | Insert individual no landing (backup ao BulkCopy) |
| `usp_clean_yellow_trip_data` | staging | `TRY_CAST` para conversão segura, calcula duração, gera hash SHA2_256 |
| `usp_reject_invalid_yellow_trip_data` | staging | Valida 11 regras de negócio, move inválidos para YellowTripRejected |
| `usp_deduplicate_yellow_trip_data` | staging | Marca duplicados intra-batch (ROW_NUMBER) e cross-batch (contra core.Trip) |
| `usp_load_trip` | core | Insere apenas `is_duplicate = 0` com verificação final contra core.Trip |

Todas as procedures são **idempotentes**: limpam dados do batch antes de reprocessar.

---

## Regras de Validação

A procedure `staging.usp_reject_invalid_yellow_trip_data` aplica 11 regras:

| Código | Regra | Campo |
|--------|-------|-------|
| R001 | pickup_datetime não pode ser NULL ou inválido | `pickup_datetime` |
| R002 | dropoff_datetime não pode ser NULL ou inválido | `dropoff_datetime` |
| R003 | dropoff deve ser posterior ao pickup | `pickup/dropoff` |
| R004 | trip_distance não pode ser negativa | `trip_distance` |
| R005 | fare_amount não pode ser negativo | `fare_amount` |
| R006 | total_amount não pode ser negativo | `total_amount` |
| R007 | passenger_count deve estar entre 0 e 9 | `passenger_count` |
| R008 | Duração da corrida não pode exceder 12 horas (720 min) | calculado |
| R009 | total_amount não pode ser menor que fare_amount | `total/fare` |
| R010 | pickup_location_id deve estar entre 1 e 265 | `pickup_location_id` |
| R011 | dropoff_location_id deve estar entre 1 e 265 | `dropoff_location_id` |

Registros rejeitados são salvos com **todas** as regras violadas em `rejection_reason` e o **código da primeira** em `rejection_rule`.

---

## Deduplicação

A deduplicação acontece em **dois níveis**:

### 1. Intra-batch (dentro do mesmo lote)

Registros com o mesmo `row_hash` dentro do batch — mantém o de menor `clean_id`, marca os demais como `is_duplicate = 1`.

Usa `ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY clean_id)`.

### 2. Cross-batch (contra dados já carregados)

Se o `row_hash` já existe em `core.Trip`, marca como duplicado. Garante idempotência: reprocessar o mesmo arquivo não gera duplicatas.

### Composição do hash (SHA2_256)

```sql
HASHBYTES('SHA2_256', CONCAT(
    vendor_id, '|', pickup_datetime, '|', dropoff_datetime, '|',
    passenger_count, '|', trip_distance, '|',
    pickup_location_id, '|', dropoff_location_id, '|',
    fare_amount, '|', total_amount
))
```

O índice `UNIQUE` em `core.Trip.row_hash` é a **garantia final** contra duplicatas.

---

## Projetos C#

### TaxiPipeline.Domain

Camada de domínio sem dependências externas.

| Arquivo | Descrição |
|---------|-----------|
| `AppSettings.cs` | POCO com todas as configurações: connection string, diretórios, API, limites |
| `BatchContext.cs` | Contexto de execução com BatchId e 5 métricas de contagem |
| `TripRecord.cs` | Entidade com 19 campos como `string?` + `SourceLineNumber` |
| `BatchStatus.cs` | Enum: Started, Completed, Failed, Reprocessing |
| `PipelineStep.cs` | Enum: 8 etapas do pipeline |
| `I*Service.cs` | 7 interfaces que definem os contratos do sistema |

### TaxiPipeline.Application

| Arquivo | Descrição |
|---------|-----------|
| `PipelineOrchestrator.cs` | Executa as 8 etapas em sequência. Métodos: `RunAsync` (1 arquivo), `RunAllAsync` (todos), `RunFromApiAsync` (API). Error handling com try/catch + logging para banco |

### TaxiPipeline.Infrastructure

| Arquivo | Descrição |
|---------|-----------|
| `TaxiApiService.cs` | Download streaming de Parquet do CDN NYC TLC com progresso no console, cache local, download atômico (temp → rename) |
| `SqlConnectionFactory.cs` | Factory que centraliza criação de `SqlConnection` |
| `BatchService.cs` | Start/Finish batch via stored procedures |
| `RawLoadService.cs` | `SqlBulkCopy` com DataTable, mapeamento de 21 colunas, `NullIfEmpty` |
| `StoredProcedureExecutor.cs` | Execução genérica de SPs com output parameter para row count |
| `CsvFileReaderService.cs` | Parser CSV com mapeamento flexível (snake_case e PascalCase) |
| `ParquetFileReaderService.cs` | Leitor Parquet via Parquet.Net com suporte a `MaxRecordsFromApi`, formata DateTimeOffset/double/decimal para string |
| `FileReaderResolver.cs` | Composite pattern: detecta `.parquet` vs `.csv` pela extensão e delega ao leitor correto |
| `ExecutionLogger.cs` | Logging duplo: `ILogger` (console) + `ops.usp_log_error` (banco) |

### TaxiPipeline.Console

| Arquivo | Descrição |
|---------|-----------|
| `Program.cs` | Entry point: configura DI, carrega settings, parseia CLI (`--api`, `--help`), trata Ctrl+C com `CancellationToken` |

---

## Configurações

O arquivo `appsettings.json` contém as configurações base (sem credenciais):

```json
{
  "Pipeline": {
    "ConnectionString": "Server=YOUR_SERVER;Database=TaxiPipelineDB;User Id=YOUR_USER;Password=YOUR_PASSWORD;TrustServerCertificate=True;",
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

### Tabela de Configurações

| Setting | Descrição | Padrão |
|---------|-----------|--------|
| `ConnectionString` | String de conexão SQL Server | (placeholder) |
| `InputDirectory` | Diretório dos arquivos de entrada | `data/input` |
| `ArchiveDirectory` | Diretório para arquivar processados | `data/archive` |
| `DownloadDirectory` | Diretório para downloads da API | `data/input` |
| `FilePattern` | Padrão glob para buscar arquivos | `*.csv` |
| `BulkCopyBatchSize` | Tamanho do lote do SqlBulkCopy | 5000 |
| `BulkCopyTimeoutSeconds` | Timeout do BulkCopy em segundos | 120 |
| `ArchiveAfterProcessing` | Mover arquivo para archive após processar | true |
| `CsvDelimiter` | Delimitador do CSV | `,` |
| `ApiBaseUrl` | URL base do CDN NYC TLC | cloudfront.net |
| `ApiFileNamePattern` | Padrão do nome do arquivo na API | `yellow_tripdata_{year}-{month}.parquet` |
| `MaxRecordsFromApi` | Limite de registros do Parquet (0 = sem limite) | 10000 |

### Hierarquia de Configuração (ordem de prioridade)

```
1. appsettings.json               ← Base (commitado no Git, sem credenciais)
2. appsettings.Development.json   ← Sobrescreve (gitignored — credenciais aqui)
3. appsettings.Local.json         ← Sobrescreve (gitignored — alternativa)
4. Variáveis de ambiente          ← Prefixo TAXIPIPELINE_ (maior prioridade)
```

Exemplo com variável de ambiente:

```powershell
$env:TAXIPIPELINE_Pipeline__ConnectionString = "Server=meuservidor;Database=TaxiPipelineDB;..."
```

---

## Segurança — Proteção de Credenciais

| Mecanismo | Detalhes |
|-----------|----------|
| `appsettings.json` | Contém apenas **placeholders** (`YOUR_SERVER`, `YOUR_USER`, `YOUR_PASSWORD`) |
| `appsettings.Development.json` | Credenciais reais — **ignorado pelo Git** via `.gitignore` |
| `appsettings.Local.json` | Alternativa local — também **ignorado pelo Git** |
| Variáveis de ambiente | Prefixo `TAXIPIPELINE_` — sobrescreve qualquer arquivo |
| `.gitignore` | Exclui `**/appsettings.Development.json`, `**/appsettings.Local.json`, dados baixados |

> **Nunca commite credenciais reais no `appsettings.json`.**

---

## Consultas de Verificação

Após executar o pipeline, use estas queries para verificar os resultados:

```sql
-- Status dos batches
SELECT batch_id, source_file_name, batch_status,
       total_rows_read, total_rows_landed, total_rows_cleaned,
       total_rows_rejected, total_rows_loaded,
       started_at, finished_at
FROM ops.BatchControl
ORDER BY batch_id DESC;

-- Log de execução do último batch
SELECT step_name, step_status, rows_affected, message,
       started_at, finished_at
FROM ops.ExecutionLog
WHERE batch_id = (SELECT MAX(batch_id) FROM ops.BatchControl)
ORDER BY log_id;

-- Total de corridas carregadas
SELECT COUNT(*) AS total_trips FROM core.Trip;

-- Rejeições agrupadas por regra
SELECT rejection_rule, COUNT(*) AS qty
FROM staging.YellowTripRejected
GROUP BY rejection_rule
ORDER BY rejection_rule;

-- Problemas de qualidade
SELECT rule_name, severity, issue_description, affected_rows
FROM ops.DataQualityIssue
WHERE batch_id = (SELECT MAX(batch_id) FROM ops.BatchControl);

-- Erros (se houver)
SELECT step_name, error_message, occurred_at
FROM ops.ExecutionError
WHERE batch_id = (SELECT MAX(batch_id) FROM ops.BatchControl);

-- Amostra dos dados finais
SELECT TOP 10
    pickup_datetime, dropoff_datetime,
    trip_distance, fare_amount, total_amount,
    trip_duration_minutes, pickup_location_id, dropoff_location_id
FROM core.Trip
ORDER BY trip_id DESC;
```

---

## Decisões Técnicas

| Decisão | Justificativa |
|---------|---------------|
| **Landing com NVARCHAR** | Preserva o dado original sem perda por conversão prematura |
| **TRY_CAST no staging** | Conversão segura que retorna NULL em vez de erro |
| **SqlBulkCopy** | Performance ordens de magnitude superior ao INSERT row-by-row |
| **SHA2_256 para hash** | Assinatura de 256 bits — probabilidade de colisão praticamente zero |
| **Idempotência** | Toda procedure limpa dados do batch antes de reprocessar |
| **4 schemas** | Separação clara: landing (bruto), staging (processado), core (final), ops (controle) |
| **Injeção de dependência** | Facilita testes e troca de implementações |
| **Logging duplo** | Console para dev, tabelas `ops` para auditoria em produção |
| **FileReaderResolver** | Composite pattern — detecta formato automaticamente |
| **Download atômico** | Arquivo `.downloading` temporário, renomeia ao concluir |
| **Colunas nullable no core** | Dados reais da NYC têm valores nulos em campos numéricos |

---

## Melhorias Futuras

- [ ] Processamento paralelo de múltiplos arquivos
- [ ] Dashboard de monitoramento com métricas de qualidade
- [ ] Testes unitários e de integração
- [ ] Containerização com Docker + Docker Compose (SQL Server + app)
- [ ] CI/CD com GitHub Actions
- [ ] Notificações por email/Slack em caso de falha
- [ ] Particionamento de tabelas por data no `core.Trip`
- [ ] SCD (Slowly Changing Dimensions) para dados de referência
- [ ] Suporte a carga incremental por watermark
- [ ] Health check endpoint para monitoramento
- [ ] Migração de schema automática (DbUp ou Flyway)
- [ ] Suporte a múltiplos tipos de táxi (Green, FHV)

---

## Fonte de Dados

Os dados são provenientes da **NYC Taxi & Limousine Commission (TLC)**:

| Informação | Valor |
|------------|-------|
| Site oficial | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| CDN (Parquet) | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| Período disponível | 2009 até ~2 meses atrás |
| Volume mensal | ~3 milhões de registros (Yellow Taxi) |
| Formato na API | Apache Parquet |

---

## Licença

Projeto para fins educacionais e de portfólio.
