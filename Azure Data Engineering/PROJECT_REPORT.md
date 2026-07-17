# Project Report: Spotify Analytics — End-to-End Azure Data Engineering Pipeline

**Type:** Personal portfolio project (solo build)
**Domain:** Music streaming analytics (Spotify-style)
**Source data:** Azure SQL star schema — 500 users, 500 artists, 500 tracks, 365 calendar dates, 1,000+ stream events, plus a separate simulated incremental batch

---

## 1. Problem Statement

Most portfolio ETL projects stop at "copy data, run a notebook, load a table." This project was scoped deliberately further, to demonstrate patterns that separate a script from a *platform*: incremental (CDC) loading instead of full reloads, one metadata-driven pipeline instead of one-pipeline-per-table, schema-evolving stream ingestion instead of brittle fixed schemas, declarative data quality instead of ad-hoc `if` checks, and historical tracking (SCD Type 2) instead of overwrite-in-place — all deployed through a real dev/prod release process.

## 2. Objectives

- Model a realistic streaming-service star schema (users, artists, tracks, dates, stream events) in Azure SQL as the system of record
- Ingest incrementally using a CDC watermark, through a single reusable, parameter-driven ADF pipeline — not five bespoke ones
- Process Bronze → Silver with Databricks Autoloader so new files and evolving schemas are picked up automatically
- Process Silver → Gold with Delta Live Tables, using declarative data-quality expectations and CDC-aware merges
- Preserve full change history on slowly-changing dimensions (SCD Type 2) while keeping the fact table latest-state-only (SCD Type 1)
- Serve the result as a proper Star Schema in Unity Catalog
- Deploy everything through Databricks Asset Bundles with distinct dev/prod targets, and promote ADF changes through Git pull requests
- Alert automatically on pipeline failure via Logic Apps

## 3. Architecture Decisions & Rationale

| Decision | Why |
|---|---|
| **Azure SQL as source instead of flat files** | Better simulates a real OLTP-style upstream system with an `updated_at`/`stream_timestamp` change-tracking column — the norm for CDC-based pipelines in enterprise data engineering, versus assuming data always arrives as clean batch files. |
| **One parameterized ADF pipeline + ForEach loop, instead of 5 separate pipelines** | Demonstrates a metadata-driven framework: the pipeline logic (Lookup watermark → Copy → branch on new data → update watermark) is written once and driven by a config array (`schema`, `table`, `cdc_col`). Adding a 6th source table is a config change, not new pipeline development — directly relevant to how larger ADF estates avoid pipeline sprawl. |
| **CDC watermarking (per-table `cdc_col`, sentinel `1900-01-01`) over full reloads** | Avoids re-copying the entire warehouse on every run; only rows changed since the last successful run are moved, which is both cheaper and faster as the dataset grows. |
| **Databricks Autoloader (`cloudFiles`) instead of a static batch read** | Handles incremental file discovery and schema evolution (`addNewColumns`) automatically, so a new column appearing in Bronze doesn't break the Silver job — a real operational concern for evolving upstream schemas. |
| **`trigger(once=True)` micro-batches instead of an always-on streaming cluster** | Keeps compute cost near zero between runs while still using genuine Structured Streaming APIs and checkpointing — appropriate for a portfolio project's budget, and a legitimate pattern for low-frequency streaming workloads in production too. |
| **Delta Live Tables over hand-written MERGE INTO logic** | `dlt.create_auto_cdc_flow` declaratively handles the upsert/SCD logic per table (just key + sequence column + SCD type), and `dlt.expect_all_or_drop` adds enforced data-quality gates without imperative validation code — less code, fewer bugs, self-documenting pipeline intent. |
| **SCD Type 2 on dimensions, SCD Type 1 on the fact table** | `DimUser`, `DimTrack`, and `DimDate` need historical accuracy (e.g., "what subscription tier was this user on when they streamed this track?"), so they retain full change history. `FactStream` is an immutable event log where only the latest state per key matters, so Type 1 (no history) is the correct, cheaper choice — the report treats this as a deliberate modeling decision, not an oversight. |
| **Unity Catalog with Bronze/Silver/Gold as schemas within one catalog (`spotify_data`)** | Keeps medallion layers logically separated but centrally governed, with the Gold schema exposed as the Star Schema business layer for downstream consumption. |
| **Jinja2-templated SQL for multi-table joins** | Extends the "metadata-driven" philosophy from ingestion into querying — join conditions and column lists live in a config list, and the query itself is generated, so adding a new joined dimension doesn't mean hand-writing new SQL each time. |
| **Databricks Asset Bundles (dev/prod targets) + GitHub PR-based ADF promotion** | Mirrors how a real data team ships changes: Databricks logic is bundled and deployed per environment with distinct permissions and workspace paths, while ADF's native Git integration promotes changes through reviewed pull requests instead of editing resources directly in a shared factory. |

## 4. Data

The warehouse is a clean Kimball-style star schema:

- **`DimUser`** (500 rows) — user_id, user_name, country, subscription_type (`Free`/`Premium`/`Family`), start/end date, `updated_at`
- **`DimArtist`** (500 rows) — artist_id, artist_name, genre, country, `updated_at`
- **`DimTrack`** (500 rows) — track_id, track_name, artist_id, album_name, duration_sec, release_date, `updated_at`
- **`DimDate`** (365 rows) — date_key, date, day, month, year, weekday
- **`FactStream`** (1,000+ rows, plus a 300-row incremental batch) — stream_id, user_id, track_id, date_key, listen_duration, device_type, stream_timestamp

A second script (`spotify_incremental_load.sql`) simulates a later batch of new/changed records across all five tables, used to validate that the CDC watermark and SCD logic correctly pick up and version only what changed.

## 5. Pipeline Detail

### 5.1 Source & CDC config
Each table's change-tracking column is declared once, centrally, in a JSON array (`loop_input`) consumed by the ForEach pipeline:
```json
{ "schema": "dbo", "table": "DimUser", "cdc_col": "updated_at", "from_date": "" }
```
A separate `cdc.json` seeds the very first run with a `1900-01-01` sentinel so the initial load pulls everything, and every subsequent run only pulls what changed since the last stored watermark.

### 5.2 Ingestion (Azure Data Factory)
**`incremental_ingestion`** (parameterized on `schema`, `table`):
1. **Lookup** — fetch the last stored CDC watermark for this table
2. **Set Variable** — hold the current run's cutoff
3. **Copy Data** (`AzureSqlToLake`) — dynamic source query filtered on the watermark, sink to ADLS Bronze
4. **If Condition** (`If_Incremental_Data`) — **true**: compute new max watermark, update stored value; **false**: delete the empty output file so Bronze stays clean

**`incremental_ingestion_loop`** wraps the above in a **ForEach** driven by the JSON config, so all 5 tables run through identical logic in one orchestrated pass, with a **Web** alert activity wired to the failure path.

### 5.3 Bronze → Silver (Databricks Autoloader)
The `silver_dimsensions` notebook processes each table with the same pattern: Autoloader read (`cloudFiles`, schema evolution on) → drop Autoloader's internal `_rescued_data` column via a shared `reusable` utility class → de-duplicate on the business key → light transformation (e.g. `DimTrack.duration_sec` bucketed into a `durationflag`, `track_name` regex-cleaned, `DimUser.user_name` uppercased) → write to Delta under `spotify_data.silver.*` with `trigger(once=True)` and a per-table checkpoint path.

### 5.4 Silver → Gold (Delta Live Tables)
Each Gold table is a short declarative DLT script. `DimUser` additionally enforces a data-quality expectation (`user_id IS NOT NULL`, dropping violators before merge). All four dimensions/fact use `dlt.create_auto_cdc_flow` with a natural key and a sequencing column to drive the CDC-aware merge — three as SCD Type 2 (full history) and `FactStream` as SCD Type 1 (latest state only).

### 5.5 Metadata-driven querying (Jinja2)
A separate exploration notebook renders a parameterized join (`FactStream` ⟕ `DimUser` ⟕ `DimTrack`) from a Python config list through a Jinja2 template — proving the same metadata-driven principle applies to consumption, not just ingestion.

### 5.6 CI/CD
ADF resources are Git-connected; a representative PR (`Dev` → `main`, 14 commits, 11 files changed) shows the linked services (`Azure_Sql`, `Azure_Data_Lake_Storage`), dynamic datasets (`Json_Dynamic`, `Parquet_Dynamic`), and both ingestion pipelines being reviewed and merged together as one unit of work. On the Databricks side, `databricks.yml` defines a `dev` target (development mode, user-prefixed resources, paused schedules) and a `prod` target (fixed workspace path, explicit `CAN_MANAGE` permission grant) sharing the same `catalog`/`schema` bundle variables — so one bundle definition deploys safely to both environments.

### 5.7 Alerting
The ADF pipeline's failure path calls an Azure Logic App (Gmail connector), so a failed run surfaces as an email without anyone needing to check the ADF monitoring UI.

## 6. Challenges & How They Were Addressed

- **Avoiding pipeline sprawl across 5 source tables** → solved with a single parameterized pipeline driven by a metadata array, rather than duplicating pipeline logic per table.
- **Handling schema drift between Bronze and Silver without breaking the job** → solved with Databricks Autoloader's `addNewColumns` schema evolution mode instead of a fixed, manually-maintained schema.
- **Keeping the fact table cheap while giving dimensions full auditability** → solved by deliberately mixing SCD types (Type 2 for dimensions, Type 1 for the fact) rather than defaulting to one strategy everywhere.
- **Avoiding hand-written, error-prone MERGE/upsert SQL for CDC** → solved with Delta Live Tables' `create_auto_cdc_flow`, which encodes key + sequence + SCD type declaratively.
- **Keeping dev and prod Databricks deployments in sync without duplicated code** → solved with a single Databricks Asset Bundle definition parameterized by target (`dev`/`prod`), differing only in workspace path, permissions, and schema variable.

## 7. Skills Demonstrated

`Azure Data Factory (metadata-driven pipelines, ForEach, Lookup, dynamic datasets)` · `CDC / incremental loading` · `Azure SQL Database` · `Azure Databricks` · `Structured Streaming & Autoloader` · `Delta Live Tables` · `Declarative data quality expectations` · `Slowly Changing Dimensions (Type 1 & Type 2)` · `Unity Catalog` · `Star Schema / Kimball modeling` · `Jinja2 templated SQL generation` · `Databricks Asset Bundles (dev/prod CI/CD)` · `GitHub PR-based infrastructure promotion` · `Azure Logic Apps alerting`

## 8. Future Work

- CI (GitHub Actions) to auto-run `databricks bundle deploy -t prod` on merge to `main`
- Expand DLT expectations to all Gold tables, including `expect_all_or_fail` on business-critical fields
- Add a BI/reporting layer on top of the Gold Star Schema
- Build automated backfill tooling to reprocess arbitrary historical CDC windows on demand
- Add row-count and freshness monitoring per layer, alerted through the existing Logic App

---

## Appendix: Resume-Ready Bullet Points

- Architected an end-to-end Azure data platform (Azure SQL → Data Factory → Databricks → Unity Catalog) implementing the Medallion architecture (Bronze/Silver/Gold) for a 5-table streaming analytics star schema.
- Built a metadata-driven, parameter-driven Azure Data Factory pipeline with CDC watermarking and a ForEach-based loop, replacing 5 potential per-table pipelines with a single reusable, config-driven pattern.
- Implemented Spark Structured Streaming with Databricks Autoloader for schema-evolving, incremental Bronze-to-Silver ingestion, using `trigger(once=True)` micro-batches to minimize compute cost.
- Designed and deployed Delta Live Tables pipelines with declarative data-quality expectations and CDC-aware auto-merge flows, applying SCD Type 2 history tracking on dimension tables and SCD Type 1 on the fact table.
- Modeled a Kimball-style Star Schema in Unity Catalog (4 dimensions + 1 fact) to support downstream analytical queries.
- Built a metadata-driven SQL query framework using Jinja2 templating to dynamically generate multi-table joins from configuration instead of hand-written SQL.
- Deployed all Databricks workloads via Databricks Asset Bundles with distinct dev/prod targets, and promoted Azure Data Factory changes through GitHub pull requests for reviewed, auditable infrastructure changes.
- Integrated Azure Logic Apps with the Data Factory pipeline's failure path for automated, real-time email alerting on pipeline errors.
