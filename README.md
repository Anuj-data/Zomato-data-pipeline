# Zomato AI Data Engineering — End-to-End Project

A complete batch data pipeline that takes Zomato-style food delivery data from raw CSVs all the way to AI-powered analytics:

**Zomato/Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (OpenAI)**

The dataset lands in an S3 data lake and flows into Snowflake, where dbt transforms it through medallion layers — RAW (Bronze) tables loaded via `COPY INTO`, cleaned STAGING (Silver) views, and business-ready MARTS (Gold) with dimensions, incremental facts, and aggregate marts. Apache Airflow orchestrates the whole pipeline as one daily DAG. On top of the warehouse sits an AI lane powered by OpenAI: LLM enrichment turns free-text reviews into structured, queryable columns; RAG lets you chat with your reviews; and text-to-SQL lets you query the warehouse in plain English. Streamlit serves the dashboards and AI apps.

## Why I Built This

I wanted to go beyond a toy dataset and build something that actually mirrors a production pipeline — including the messy parts. This project isn't just "load CSV, run dbt, done" — it involved real infrastructure decisions: choosing between Storage Integration vs access keys for S3, debugging IAM trust policies, running Airflow under real disk-space constraints, and deciding where AI genuinely adds value in a warehouse (enrichment, RAG, text-to-SQL) versus where it's just a gimmick.

## Architecture

📂 **Dataset + project slides:** Google Drive folder — download the CSVs here and place them under `data/` (they're too large to commit to the repo).

## What Gets Built

| Layer | Where | What |
|---|---|---|
| Source | `data/` (local) | 4 real dimension CSVs (restaurants, users, food, menu) + 3 generated fact files: 10M orders, ~23M order items, 300K free-text reviews |
| Lake | Amazon S3 | One bucket, `raw/<table>/` folder per CSV |
| Bronze | Snowflake `ZOMATO.RAW` | `COPY INTO` from S3 via an external stage |
| Silver | Snowflake `ZOMATO.STAGING` | dbt staging views — clean, type, rename every source |
| Gold | Snowflake `ZOMATO.MARTS` | Dimensions, incremental facts (MERGE), business marts + an SCD2 snapshot |
| AI | Snowflake `ZOMATO.AI` | LLM-enriched reviews (sentiment/topic), RAG chat, text-to-SQL |
| Orchestration | Airflow (Docker) | One daily DAG: load → transform → enrich → AI mart |

## Tech Stack

Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · OpenAI (gpt-4o-mini, text-embedding-3-small) · Streamlit

## Repository Structure

- **`airflow/`** — Airflow 3 on Docker
  - `Dockerfile` — Snowflake + OpenAI providers, dbt in its own venv
  - `docker-compose.yaml` — postgres + api-server + scheduler; creds via env vars
  - `example.env` — template for SNOWFLAKE_* / OPENAI_API_KEY
  - `dags/zomato_batch.py` — the pipeline DAG (4 tasks)

- **`zomato/`** — dbt project
  - `models/staging/` — 7 staging views (Silver) + sources + tests
  - `models/marts/` — dims, incremental facts, business marts (Gold)
  - `macros/` — custom schema-name macro

- **`ai/`** — AI layer
  - `enrich_reviews.py` — LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
  - `rag_chat.py` — RAG, "chat with your reviews" (Streamlit)
  - `text_to_sql.py` — text-to-SQL, "chat with your warehouse" (Streamlit)
  - `example.env` — template for the AI credentials

- **`snowflake/`** — Snowflake setup SQL (run in Snowsight, in order)
  - `01_setup.sql` — warehouse ZOMATO_WH, database ZOMATO, schemas, role
  - `02_storage_integration.sql` — S3 link (pairs with `aws/iam/`)
  - `03_stage_and_formats.sql` — external stage + CSV file format
  - `04_raw_tables.sql` — RAW (Bronze) table DDL, column order matches the CSVs
  - `05_copy_into.sql` — COPY INTO RAW from the stage

- **`aws/iam/`** — IAM policy + role trust policies for the S3 ↔ Snowflake handshake

- **`docs/architecture.png`** — architecture diagram

> `data/` (~2.3 GB of CSVs), logs, and dbt `target/` artifacts are intentionally not committed — get the dataset and slides from the Google Drive folder.
## How the Pipeline Works

### 1 · Data lands in S3
The seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/` — one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2 · S3 → Snowflake
Snowflake reads the bucket through an external stage. There are two ways to wire this up, and I ended up needing both:

- **Storage Integration + IAM role** (`snowflake/02_storage_integration.sql`, `aws/iam/`) — the "correct" production pattern: no long-lived keys, Snowflake assumes a role via `sts:AssumeRole` using an external ID.
- **IAM user + access keys** — a fallback I had to use because my AWS account (a newer "Free Plan" account) has account-level Service Control Policy guardrails that block cross-account `sts:AssumeRole` by default. The trust policy was correct, the external ID matched, and it still failed — because the restriction lives at the AWS account level, not in anything Snowflake-side. Both setups are documented in `aws/iam/`, in case you hit the same wall.

Two lessons from getting the Storage Integration path right: the trust policy's `Principal` must be Snowflake's IAM **user** ARN (from `DESC INTEGRATION`), not `:root` — and never re-run `CREATE OR REPLACE` on the integration afterward, since it regenerates the external ID and silently breaks the trust relationship you just set up.

### 3 · Load — COPY INTO
Table DDL (`snowflake/04_raw_tables.sql`) matches each CSV's column order, then `snowflake/05_copy_into.sql` pulls each file from the stage into `ZOMATO.RAW` tables: 10M orders, ~23M order items, 300K reviews.

### 4 · Transform — dbt (medallion)
- **Staging (Silver)** — one view per source: parse the messy restaurant dimension (`--` → null, `₹ 200` → `200`), lowercase emails, derive `is_delivered`, etc.
- **Dimensions (Gold)** — `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, a generated `dim_date` calendar.
- **Facts (Gold, incremental)** — `fct_orders` and `fact_order_items` use `materialized='incremental'` with a MERGE strategy, so a re-run processes only new rows instead of rebuilding 10M+.
- **Marts (Gold)** — one table per business question: daily city revenue (GMV/AOV/cancel rate), restaurant performance, delivery SLA (p50/p90 by city & hour), review insights.
- **Tests** — `unique` / `not_null` / `relationships` / `accepted_values` plus a singular reconciliation test; `dbt build` runs models and tests in dependency order.

### 5 · Orchestrate — Airflow
One daily DAG, `zomato_batch`, runs the whole thing as a single graph:
