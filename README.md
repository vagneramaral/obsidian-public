# Zig Data Pipeline

Plataforma enterprise de engenharia de dados construída com **Dagster**, **dbt**, **ClickHouse**, **Snowflake**, **FastAPI** e **Terraform**, desenvolvida para ingerir, orquestrar e transformar dados de múltiplas fontes para analytics e business intelligence.

---

## Visão Geral

Este projeto é o núcleo técnico do time de Dados da Zig. Ele cobre toda a cadeia de valor dos dados: da captura em bancos transacionais dos produtos até a entrega de modelos analíticos prontos para consumo no Metabase, Power BI e APIs internas.

### Componentes Principais

| Componente | Tecnologia | Finalidade |
|---|---|---|
| Orquestração | Dagster (5 code locations) | Execução e monitoramento de todos os pipelines |
| Transformação | dbt (30+ schemas, 1.400+ modelos) | Modelagem SQL camada a camada |
| Data Warehouses | Snowflake, Redshift, ClickHouse, Athena | Armazenamento analítico multi-engine |
| Infraestrutura Snowflake | Terraform + Spacelift | RBAC, usuários, schemas, warehouses |
| APIs & Webhooks | FastAPI | Recepção de eventos em tempo real |
| AI Assistants | MCP Server + Claude Agents | Integração de LLMs com a plataforma |
| CI/CD | GitLab CI + Helm + Kubernetes | Build, validação e deploy automáticos |

---

## Arquitetura

### Visão Completa da Plataforma

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FONTES DE DADOS                                    │
│                                                                             │
│  Produto Z (PostgreSQL)   Produto N (MSSQL)    Produto S (MySQL)            │
│  Payment, Client, Venue   Net, Finance         Sup, Operations              │
└────────────────┬──────────────────┬─────────────────────┬───────────────────┘
                 │                  │                     │
                 └──────────────────┴─────────────────────┘
                                    │
                            AWS DMS (CDC)
                         Change Data Capture
                         Replicação contínua
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          S3 — LANDING ZONE                                  │
│                                                                             │
│  s3://zig-landing/product-z/   s3://zig-landing/product-n/                  │
│  s3://zig-landing/product-s/   s3://zig-landing/external-apis/              │
└───────────────────┬─────────────────────────────────────────────────────────┘
                    │
          ┌─────────┴────────────┐
          │                      │
          ▼                      ▼
  AWS Athena (Lakehouse)   Snowflake
  Query direta no S3       Snowpipe (tabelas em cdc_tables)
  via Iceberg/Parquet      + dbt (demais tabelas)
          │                      │
          └──────────┬───────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DAGSTER — ORQUESTRAÇÃO                                  │
│                                                                             │
│  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │   main    │  │   dev    │  │    ml    │  │   fin    │  │  revops  │    │
│  │ (prod)    │  │ (staging)│  │ (ML/AI)  │  │(Finance) │  │ (RevOps) │    │
│  └───────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
│                                                                             │
│  Assets: 38 domínios (finance, sales, cs, logistics, ml, people, ...)      │
│  Shared Libs: 24 módulos de integração (AWS, Snowflake, Sharepoint, ...)   │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      dbt — TRANSFORMAÇÃO SQL                                │
│                                                                             │
│  staging → intermediate → marts → metabase/fct + metabase/dim              │
│                                                                             │
│  Schemas: finance, sales, cs, operation, people, ml_feature_store, kpi,    │
│           rfd, dwh, metabase, data, revops, risk, crm, cdp, ...            │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DATA WAREHOUSES                                        │
│                                                                             │
│  ┌──────────────┐  ┌───────────┐  ┌───────────┐  ┌──────────────────────┐  │
│  │   Snowflake  │  │  Redshift │  │ClickHouse │  │  Athena (S3 Lakehouse│  │
│  │ Analytics,ML │  │ Produção  │  │ Realtime  │  │  Landing, Staging)   │  │
│  │ Feature Store│  │ Principal │  │ Analytics │  │                      │  │
│  └──────────────┘  └───────────┘  └───────────┘  └──────────────────────┘  │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   BUSINESS INTELLIGENCE & APIs                              │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ Metabase │  │ Power BI │  │  Trino   │  │  Claude  │  │  FastAPI     │  │
│  │          │  │          │  │ (Query   │  │  Agents  │  │  Webhooks    │  │
│  │          │  │          │  │ Federation│  │  MCP     │  │  (Viva, CU) │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Ingestão via AWS DMS (CDC)

O coração da ingestão de dados são três bancos transacionais dos produtos Zig:

| Produto | Engine | Dados Principais |
|---|---|---|
| **Z (Zig)** | PostgreSQL | Pagamentos, clientes, venues, eventos |
| **N (Net)** | MSSQL | Finance, contratos, relatórios |
| **S (Sup)** | MySQL | Suporte, operações, logística |

O **AWS DMS** monitora esses bancos via CDC (Change Data Capture) e replica todas as alterações de forma contínua para o **S3 Landing Zone**. A partir do S3:

- **Athena**: queries SQL diretas nos arquivos Parquet/Iceberg do landing — base dos modelos dbt com `--profile athena`
- **Snowflake**: ingestão em dois mecanismos distintos:
  - **Snowpipe (AUTO_INGEST)**: tabelas declaradas no mapa `cdc_tables` em `.claude/zig-data-snowflake-terraform/snowpipe_cdc.tf` são ingeridas automaticamente conforme os arquivos chegam no S3, sem intervenção manual
  - **dbt**: todas as demais tabelas do S3 são lidas pelos modelos dbt via Athena (staging) e transformadas/carregadas no Snowflake nas camadas superiores

Além do CDC, assets Dagster realizam extrações adicionais via APIs externas (20+ integrações) e armazenam os dados no S3 ou diretamente nos warehouses.

### Infraestrutura Snowflake (Terraform)

O submodule `.claude/zig-data-snowflake-terraform/` contém toda a infraestrutura Snowflake como código, gerenciada via **Spacelift**:

- **120+ usuários humanos** distribuídos em 12 departamentos
- **12 bots** com autenticação RSA Key Pair para pipelines automatizados
- **8 warehouses** dedicados por uso: `DAGSTER_WH`, `DADOS_WH`, `METABASE_WH`, `POWERBI_WH`, `ZIG_USERS_WH`, `SYSTEM_WH`, `AMPLITUDE_WH`, `NEIGHTN_WH`
- **RBAC hierárquico em 3 níveis**: User Role (UR) → Functional Role (FR) → Object Role (OR) → Objetos
- **Snowpipe CDC**: tabelas declaradas em `cdc_tables` no `snowpipe_cdc.tf` recebem ingestão automática via AUTO_INGEST; demais tabelas chegam ao Snowflake via dbt

---

## Estrutura do Projeto

```
zig-data-pipeline/
├── dg.toml                              # Workspace dg CLI (5 code locations)
├── .gitlab-ci.yml                       # CI/CD pipeline completo
├── docker-compose.yml                   # Stack local completo
│
├── dagster_project/                     # Orquestração Dagster
│   ├── pyproject.toml                   # Pacote Python instalável (shared libs)
│   ├── _shared_libs/                    # 24 módulos de integração reutilizáveis
│   │   ├── aws_methods.py               # S3, DMS, Athena
│   │   ├── snowflake_methods.py         # Queries e bulk load
│   │   ├── powerbi_methods.py           # DAX, datasets, TMDL
│   │   ├── sharepoint_methods.py        # Office 365 / SharePoint
│   │   ├── netsuite_methods.py          # ERP NetSuite
│   │   └── ...                          # Slack, ClickUp, Zendesk, Gupy, etc.
│   ├── assets/                          # 38 módulos de assets por domínio
│   │   ├── finance_module/              # Adyen, Stone, Braintree, PagSeguro, etc.
│   │   ├── ml_module/                   # Forecasting, clustering, recomendações
│   │   ├── cs_module/                   # Customer Success, NPS, tickets
│   │   ├── sales_module/                # Funil, conversão, RevOps
│   │   ├── lg_module/                   # Logística (Lojas Guanabara)
│   │   ├── people_module/               # RH, folha, benefícios
│   │   └── ...
│   ├── _code_locations/                 # 5 code locations independentes
│   │   ├── base/Dockerfile              # Imagem base compartilhada
│   │   ├── main/                        # Produção (porta 4000)
│   │   ├── dev/                         # Staging/testes (porta 4001)
│   │   ├── ml/                          # Machine Learning (porta 4002)
│   │   ├── fin/                         # Finance (porta 4003)
│   │   └── revops/                      # Revenue Operations (porta 4005)
│   ├── jobs/                            # Definições de jobs
│   ├── schedules/                       # Agendamentos
│   └── sensors/                         # Sensores de dados
│
├── dbt_project/                         # Transformações SQL com dbt
│   ├── models/
│   │   ├── metabase/fct/                # Fatos para Metabase (fonte canônica)
│   │   ├── metabase/dim/                # Dimensões para Metabase
│   │   ├── rfd/                         # Refined Data (bronze→silver→gold)
│   │   ├── finance/                     # Conciliação, transações, produtos
│   │   ├── sales/                       # Funil, RevOps, metas
│   │   ├── cs/                          # Tickets, NPS, health score
│   │   ├── operation/                   # Logística, armazéns
│   │   ├── ml_feature_store/            # Features para modelos ML
│   │   ├── kpi/                         # KPIs consolidados
│   │   └── ...                          # 30+ schemas no total
│   ├── macros/                          # Macros reutilizáveis
│   ├── seeds/                           # Dados estáticos
│   └── profiles.yml                     # Conexões (redshift, snowflake, athena)
│
├── fastapi_project/                     # APIs e Webhooks
│   ├── app/
│   │   ├── webhook_listener.py          # Viva Wallet, ClickUp, etc.
│   │   └── viva_webhook.py
│   └── docker-compose-webhook.yml
│
├── data_mcp_project/                    # MCP Server para AI assistants
│   └── src/tools/                       # Power BI, Snowflake, Dagster
│
├── legacy_project/clickhouse_project/  # Cluster ClickHouse (legado)
│
├── deploy/                              # Kubernetes + Pulumi (IaC)
│
├── scripts/
│   ├── setup_agents.py                  # Setup guiado de dependências AI
│   └── lineage/                         # Análise de linhagem dbt
│
└── .claude/                             # Agentes e skills de IA
    ├── zig-agents/                      # Submodule: agentes e skills gerais
    ├── zig-data-snowflake-terraform/    # Submodule: IaC Snowflake
    ├── zig-data-presentation/           # Submodule: apresentações
    ├── agents/                          # Agentes específicos do projeto de dados
    └── skills/                          # Skills específicas do projeto de dados
```

---

## Setup para Desenvolvimento Local

### Pré-requisitos

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (gerenciador de pacotes e ambientes virtuais)
- Docker e Docker Compose
- AWS CLI configurado
- Acesso ao GitLab container registry
- Credenciais nos arquivos `.env` (ver seção de variáveis de ambiente)

### 0. Instalar o uv

O projeto usa **uv** como gerenciador de pacotes e ambientes virtuais. Ele é significativamente mais rápido que pip e resolve dependências de forma determinística.

```bash
# Linux / macOS (recomendado)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Ou via pip (se já tiver Python)
pip install uv

# Verificar instalação
uv --version
```

> O `uv` cria e gerencia o venv automaticamente ao usar `uv sync` ou `uv pip install`. Não é necessário ativar o venv manualmente para a maioria dos comandos — use `uv run <comando>` para executar dentro do ambiente.

### 1. Criar ambiente virtual Python

```bash
# Na raiz do projeto — cria o .venv com Python 3.11
uv venv --python 3.11

# Ativar o venv (necessário para comandos que não usam `uv run`)
source .venv/bin/activate
```

### 2. Instalar dependências do Dagster

```bash
# Instalar dependências base
uv pip install -r requirements.txt

# Ferramentas Dagster
uv pip install dagster-dg-cli dagster-components hatchling

# Instalar shared libs e todos os code locations como pacotes editáveis
uv pip install -e ./dagster_project/ \
               -e ./dagster_project/_code_locations/main \
               -e ./dagster_project/_code_locations/dev \
               -e ./dagster_project/_code_locations/ml \
               -e ./dagster_project/_code_locations/fin \
               -e ./dagster_project/_code_locations/revops
```

### 3. Instalar dependências do dbt

```bash
cd dbt_project

# Instalar adaptadores dbt
uv pip install dbt-core dbt-redshift dbt-snowflake dbt-athena-community dbt-clickhouse

# Instalar pacotes dbt (dbt_utils, etc.)
dbt deps --profiles-dir .

# Testar conexão
dbt debug --profiles-dir .
```

### 4. Configurar variáveis de ambiente

O repositório inclui um `.env.example` com todas as variáveis necessárias e valores fictícios de exemplo. Copie-o e preencha com as credenciais reais:

```bash
cp .env.example .env
```

> **Nunca commite o arquivo `.env`** — ele está no `.gitignore`. O `.env.example` é o documento de referência para onboarding; atualize-o sempre que uma nova variável for adicionada ao projeto.

As variáveis estão organizadas por categoria no `.env.example`:

| Categoria | Exemplos de variáveis |
|---|---|
| Runtime | `RUN_ENV`, `LAKE_ENV` |
| AWS | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| Snowflake | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PKEY` |
| Bancos de dados | `PGSQL_*`, `MYSQL_*`, `MSSQL_*`, `RDSQL_*`, `CLICKHOUSE_*` |
| Microsoft / Power BI | `AZURE_*`, `POWERBI_*`, `MS_*` |
| Metabase | `METABASE_URL`, `METABASE_API_KEY` |
| Slack / ClickUp | `SLACK_BOT_TOKEN`, `CLICKUP_TOKEN` |
| Pagamentos | Adyen, Stone, Rede, Cielo, MercadoPago, GetNet, etc. |
| Integrações diversas | NetSuite, HubSpot, Zendesk, Gupy, Datadog, N8N, etc. |

### 5. Subir a UI do Dagster localmente

**Opção A — `dg dev` (recomendado para iteração rápida):**

```bash
# Todos os code locations (a partir da raiz do projeto)
cd dagster_project && dg dev
# UI disponível em http://localhost:3000

# Subir apenas um code location específico
cd dagster_project/_code_locations/main && dg dev    # main
cd dagster_project/_code_locations/dev && dg dev     # dev
cd dagster_project/_code_locations/ml && dg dev      # ml
cd dagster_project/_code_locations/fin && dg dev     # fin
cd dagster_project/_code_locations/revops && dg dev  # revops
```

**Opção B — Docker Compose (stack completo com banco de metadados):**

```bash
docker-compose up --build -d
docker-compose ps
```

**Subir somente um container específico:**

```bash
# Subir apenas o code location desejado (ex: main, dev, ml, fin, revops)
docker-compose up zig_dagster_user_code -d       # main
docker-compose up zig_dagster_dev_code -d        # dev
docker-compose up zig_dagster_ml_code -d         # ml
docker-compose up zig_dagster_fin_code -d        # fin
docker-compose up zig_dagster_revops_code -d     # revops

# Reconstruir apenas um container sem afetar os demais
docker-compose up --build zig_dagster_fin_code -d
```

### 6. Rodar modelos dbt localmente

```bash
cd dbt_project

# Modelos no schema rfd/ → Athena
dbt run -s nome_do_modelo --profile athena

# Demais schemas → Snowflake (ambiente de desenvolvimento)
dbt run -s nome_do_modelo --profile snowflake --target local

# Rodar testes
dbt test -s nome_do_modelo --profile snowflake --target local

# Documentação interativa
dbt docs generate --profiles-dir . && dbt docs serve
```

### 7. Rodar testes

```bash
# Testes unitários dos shared libs (pytest)
cd dagster_project
uv run pytest tests/ -v

# Testes dbt
cd dbt_project
dbt test --profiles-dir . --target local
```

---

## CI/CD — GitLab vs Execução Local

### Como funciona o CI/CD

O pipeline GitLab CI (`.gitlab-ci.yml`) executa **apenas na branch `main`** e é composto por 6 stages sequenciais:

```
validate-fast → build-base → build-independent → build → validate → deploy
```

| Stage | Jobs | O que faz |
|---|---|---|
| `validate-fast` | Unit tests shared libs | Roda `pytest` dos `_shared_libs` em Python 3.13 |
| `build-base` | Build imagem base | Builda `dagster_project/_code_locations/base/Dockerfile` com cache inteligente por hash SHA256 |
| `build-independent` | FastAPI + MCP | Builda imagens em paralelo (não dependem da base Dagster) |
| `build` | Main, Dev, ML, FIN, RevOps | 5 builds paralelos dos code locations |
| `validate` | dg check defs | Valida as definições Dagster de cada location em container Docker |
| `deploy` | Helm + Kubernetes | Upgrade do Helm chart Dagster + deploy de serviços auxiliares |

**Cache inteligente de imagens:** A imagem base só é reconstruída se o hash SHA256 dos arquivos determinísticos mudar. Isso evita rebuilds desnecessários a cada commit e reduz o tempo de pipeline significativamente.

### Diferenças entre local e CI/CD

| Aspecto | Local (`dg dev`) | CI/CD (GitLab) |
|---|---|---|
| **Credenciais** | `.env` na raiz | Variáveis de CI/CD criptografadas no GitLab |
| **Banco de metadados** | SQLite (padrão) ou Postgres via Docker | Postgres dedicado no Kubernetes |
| **Imagens Docker** | Build local sob demanda | Build automático, publicado no registry com tag `v$CI_PIPELINE_ID` |
| **Validação** | Manual (`dg check defs`) | Automática antes do deploy |
| **Deploy** | Não se aplica | Helm chart Dagster v1.11.16 no Kubernetes |
| **Snowflake Terraform** | `terraform plan/apply` local | Gerenciado pelo Spacelift (integrado ao GitLab) |
| **Branch** | Qualquer branch | Apenas `main` |

**Regra prática:** Desenvolva e teste localmente em branches de feature. O CI/CD cuida do deploy ao fazer merge para `main`. Nunca commite diretamente na `main`.

---

## AI Agents e Skills

O projeto conta com um framework completo de agentes e skills de IA para acelerar o trabalho de engenharia de dados. Eles vivem no submodule `.claude/zig-agents/` e nas pastas `.claude/agents/` e `.claude/skills/`.

### Dependências para usar os Agents

| Dependência | Uso | Como configurar |
|---|---|---|
| Python 3.11+ | Runtime do ambiente | `python3 --version` |
| [uv](https://docs.astral.sh/uv/) | Gerenciador de pacotes e venv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [Snowflake CLI](https://docs.snowflake.com/developer-guide/snowflake-cli) (`snow`) | Execução de SQL direto no Snowflake | `uv tool install snowflake-cli` (instalado via `make setup`) |
| `~/.snowflake/config.toml` com conexão `zig` | Autenticação SSO no Snowflake | Ver instruções abaixo |
| E-mail `@zig.fun` | SSO via browser na primeira execução | Provisionado pelo time de TI |
| `.env` com `METABASE_API_KEY` e `METABASE_URL` | Criação e consulta de cards | Pegar com o time de Dados |
| Pacote Python `requests` | Chamadas à API do Metabase | `uv pip install requests` |
| Claude Code CLI | Execução dos agentes | `npm install -g @anthropic-ai/claude-code` |

### Setup guiado (recomendado)

```bash
# Verificação completa + setup interativo
python3 scripts/setup_agents.py

# Apenas verificar sem modificar nada
python3 scripts/setup_agents.py --check

# Verificar + testar conectividade ao Snowflake e Metabase
python3 scripts/setup_agents.py --test
```

O script detecta o que está faltando e guia passo a passo. Em caso de erro, imprime o comando exato para corrigir.

### Setup manual da conexão Snowflake

```bash
# 1. Instalar Snowflake CLI (ambiente isolado — não entra no venv do projeto)
# Já executado automaticamente pelo `make setup`. Para instalar manualmente:
uv tool install snowflake-cli

# 2. Criar ~/.snowflake/config.toml
cat > ~/.snowflake/config.toml << 'EOF'
default_connection_name = "zig"

[connections.zig]
account = "<SNOWFLAKE_ACCOUNT>"
user = "seu.nome@zig.fun"
warehouse = "DADOS_WH"
authenticator = "externalbrowser"
EOF

# 3. Testar conexão (abre o browser para SSO na primeira vez)
snow connection test --connection zig

# 4. Rodar uma query de teste
snow sql --connection zig -q "SELECT CURRENT_USER(), CURRENT_WAREHOUSE()"
```

### Catálogo de Agents disponíveis

**Agentes de Dados (`.claude/agents/` e `.claude/zig-agents/agents/`):**

| Agent | Uso |
|---|---|
| `data-engineer` | Criar e modificar assets Dagster (skill: `/dagster-asset`) |
| `analytics-engineer` | Criar e modificar modelos dbt (skill: `/dbt-model`) |
| `data-insights` | Responder perguntas de negócio com dados (skill: `/sql-explore`) |
| `data-scientist` | Pipelines de ML, feature engineering, avaliação de modelos |

**Agentes Gerais (`.claude/zig-agents/agents/`):**

| Agent | Uso |
|---|---|
| `planner` | Criar planos de implementação antes de codar |
| `architect` | Decisões de design e trade-offs de arquitetura |
| `developer` | Execução disciplinada de stories e tasks |
| `debugger` | Diagnóstico e resolução de bugs |
| `qa` | Revisão de qualidade e testes |
| `product-manager` | PRDs, requisitos, handover para dev |
| `researcher` | Investigação de tecnologias e soluções |

### Catálogo de Skills disponíveis

Skills são invocadas com `/nome-da-skill` no Claude Code:

| Skill | Quando usar |
|---|---|
| `/dagster-asset` | Criar ou modificar um asset Dagster — determina o módulo, code location, padrão (`@asset` / `@multi_asset`) e configura jobs/schedules automaticamente |
| `/dbt-model` | Criar ou modificar modelos dbt — determina o domínio, profile (athena/snowflake), macros multi-db, gera SQL + schema YML com testes |
| `/dbt-docs` | Gerar ou revisar documentação dbt (schema YML) usando o SQL do modelo, linhagem e cards do Metabase como contexto |
| `/metabase-question` | Criar cards no Metabase via SQL nativo no Snowflake — busca cards similares, usa modelos dbt corretos, valida antes de criar |
| `/sql-explore` | Exploração ad-hoc de dados via SQL no Snowflake — investigar modelos dbt, verificar cobertura e qualidade |
| `/resolve-data-task` | Recebe um link de cartão ClickUp e/ou thread Slack, captura o contexto e roteia para o agent correto |
| `/triagem-pedidos` | Triagem e criação de cartões na Recepção de Pedidos do ClickUp |
| `/pm-dados-prd` | Criar PRDs para o time de Dados seguindo o template Zig |
| `/zig-brand` | Criar apresentações e materiais visuais seguindo o manual de marca |

---

## Boas Práticas com Agents e Skills

### Fluxo de trabalho recomendado

```
1. /resolve-data-task <link ClickUp>   → Captura contexto e classifica o trabalho
2. /dagster-asset  ou  /dbt-model      → Implementa o que foi classificado
3. /dbt-docs                           → Documenta o modelo criado
4. /metabase-question                  → Cria o card de visualização se necessário
```

### Boas práticas ao usar os agents

**Antes de começar:**
- Sempre forneça o link do cartão ClickUp e/ou thread Slack — o contexto da demanda é fundamental para o agent tomar boas decisões.
- Para tarefas de análise exploratória, use `/sql-explore` antes de qualquer implementação para entender o grain e a cobertura dos dados.

**Durante a implementação:**
- Prefira sempre os modelos `metabase/fct/` e `metabase/dim/` sobre modelos `rfd/` brutos ao construir queries ou cards no Metabase.
- Nunca use colunas de controle ELT em queries analíticas: `PARTITION_0`, `PARTITION_1`, `PARTITION_2`, `DMS_TIMESTAMP`, `PY_TIMESTAMP`.
- Ao criar um asset Dagster novo, verifique qual code location (`main`, `fin`, `revops`, etc.) é o mais apropriado para o domínio. Não crie assets no `dev` que deveriam estar em `main`.
- Para modelos dbt no schema `rfd/`, use sempre `--profile athena`. Para os demais schemas, use `--profile snowflake --target local`.

**Qualidade e revisão:**
- Após qualquer implementação de código, rode os testes antes de commitar (`dbt test` para modelos dbt, `pytest` para assets Dagster).
- Valide as definições Dagster antes de abrir MR: `dg check defs` no diretório do code location.
- Para modelos dbt em produção, sempre inclua testes de `not_null` e `unique` nas colunas-chave no schema YML.
- Use `/dbt-docs` para manter a documentação atualizada — o Metabase usa as descrições dos modelos dbt para contexto.

**Snowflake e permissões:**
- Alterações em usuários, roles ou schemas Snowflake devem ser feitas via Terraform em `.claude/zig-data-snowflake-terraform/` — nunca manualmente no console Snowflake.
- Para adicionar um novo bot, seguir o padrão `roles_user_bots.tf` e `users_bots.tf`. Para exceções de permissão, usar `roles_user_exceptions.tf`.

**Comunicação e rastreabilidade:**
- Toda demanda de dados deve ter um cartão no ClickUp na Recepção de Pedidos antes de ser iniciada. Use `/triagem-pedidos` para criar o cartão seguindo os campos obrigatórios.
- Ao abrir um MR, referencie o cartão ClickUp na descrição.

---

## Comandos de Referência Rápida

### Dagster

```bash
# Subir UI local (todos os locations)
cd dagster_project && dg dev

# Validar definições sem subir a UI
cd dagster_project/_code_locations/main && dg check defs

# Listar todos os assets/jobs/schedules de um location
cd dagster_project/_code_locations/main && dg list defs

# Scaffold de novo asset
cd dagster_project/_code_locations/main && dg scaffold defs
```

### dbt

```bash
cd dbt_project

# Rodar modelo específico
dbt run -s nome_do_modelo --profile athena
dbt run -s nome_do_modelo --profile snowflake --target local

# Rodar com dependências upstream (+) e downstream (nome+)
dbt run -s +nome_do_modelo --profile snowflake --target local

# Testes
dbt test -s nome_do_modelo --profile snowflake --target local

# Ver linhagem de um modelo
dbt ls -s nome_do_modelo+ --output path

# Gerar e servir documentação
dbt docs generate --profiles-dir . && dbt docs serve
```

### FastAPI (Webhooks)

```bash
# Subir localmente
docker-compose -f fastapi_project/docker-compose-webhook.yml up --build
# Docs: http://localhost:8000/docs
```

### MCP Server

```bash
cd data_mcp_project
python -m src.server
```

### Snowflake Terraform

```bash
cd .claude/zig-data-snowflake-terraform

# Planejar mudanças
terraform plan

# Aplicar (o deploy automático é via Spacelift no merge para main)
terraform apply
```

---

## Integrações Externas

O projeto integra com 30+ sistemas externos:

**Pagamentos:** Adyen, Stone, Braintree, PagSeguro, Rede, Cielo, Mercado Pago, Viva Wallet, GetNet, Cyber, Vexpense

**CRM & Vendas:** HubSpot, Pipedrive, SenseData

**Suporte & CS:** Zendesk, ClickUp, Slack

**RH:** Convenia (Gupy), LG Place

**Analytics & BI:** Metabase, Power BI, Moengage, Amplitude, Firebase

**Cloud & Infra:** AWS (S3, DMS, Athena, Redshift, Glue), Google Drive, SharePoint / Office 365

**ERP:** NetSuite, BigDataCorp

**Data Integration:** Sling, N8N

---

## Monitoramento e Observabilidade

- **Dagster UI**: http://localhost:3000 — runs, logs, asset lineage, partitions
- **dbt docs**: linhagem de modelos e documentação de colunas
- **ClickHouse System Tables**: métricas de queries e armazenamento
- **Datadog**: APM e logs de produção (via `datadog_module`)
- **Health checks**: endpoints `/health` em FastAPI e MCP Server

---

## Stack Tecnológico

| Categoria | Tecnologias |
|---|---|
| **Orquestração** | Dagster 1.11.8, dagster-dg-cli |
| **Transformação** | dbt-core 1.9+, dbt-redshift, dbt-snowflake, dbt-athena, dbt-clickhouse |
| **Data Warehouses** | Snowflake, Amazon Redshift, ClickHouse 23.11, AWS Athena, Trino |
| **Ingestão CDC** | AWS DMS, Snowpipe, S3 Lakehouse |
| **APIs** | FastAPI, Flask (legacy) |
| **Machine Learning** | scikit-learn, CatBoost, Snowflake ML, LangGraph, SHAP |
| **Infraestrutura** | Terraform, Spacelift, Kubernetes, Helm, Pulumi, Docker |
| **Cloud** | AWS (S3, DMS, Athena, Redshift, Glue), Kubernetes |
| **Bancos de Dados** | PostgreSQL, MSSQL (SQL Server), MySQL, ClickHouse |
| **AI / Agents** | Claude Code, MCP Server, LangGraph |
| **Monitoramento** | Dagster UI, dbt docs, Datadog |

---

## Documentação Adicional

- [FastAPI Webhooks](fastapi_project/README.md)
- [MCP Server](data_mcp_project/README.md)
- [ClickHouse Setup](legacy_project/clickhouse_project/README.md)
- [Webhook Testing Guide](fastapi_project/WEBHOOK_TESTING_GUIDE.md)
- [Lineage Table Builder](scripts/lineage/README.md)
- [Snowflake Terraform](/.claude/zig-data-snowflake-terraform/README.md)
- [AI Agents & Skills](/.claude/zig-agents/README.md)

---

**Desenvolvido pelo Time de Data Engineering da Zig**
