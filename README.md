# End-to-End FHIR Healthcare Medallion Data Pipeline

## 📌 Project Overview
This project implements an enterprise-grade, end-to-end Medallion Data Pipeline on Databricks using PySpark and Delta Lake. It ingests clinical data from the public HAPI FHIR API (Patient, Encounter, Observation, Condition), enforces data quality, tracks dimensional changes using SCD Type 2, and models analytical Gold layer tables.

---

## 🏗️ Architecture & Medallion Layers

        HAPI FHIR REST API
                │ (Incremental Fetch & Pagination)
                ▼
        ┌───────────────────┐
        │    RAW SCHEMA     │ Local JSON Archives + Parsed Delta Tables
        └─────────┬─────────┘
                  │ (Metadata Logging: Timestamps & API Params)
                  ▼
        ┌───────────────────┐
        │   BRONZE SCHEMA   │ Standardized Ingestion Tables
        └─────────┬─────────┘
                  │ (Foreign Key Cleaning & SCD Type 2 Merges)
                  ▼
        ┌───────────────────┐
        │   SILVER SCHEMA   │ Historical & Current State Tracking
        └─────────┬─────────┘
                  │ (Dimensional Aggregation & Data Modeling)
                  ▼
        ┌───────────────────┐
        │    GOLD SCHEMA    │ Analytics-Ready Dimension & Fact Models
        └───────────────────┘

### Layer Breakdown

1. Schema Creation Layer (00_Schema_Creation):
   * Initializes catalog schemas (raw, bronze, silver, gold).
   * Seeds the metadata control table (raw.ingestion_metadata) to define execution order dynamically.

2. Raw & Bronze Ingestion Layer (01_Raw_Bronze_Ingestion):
   * Incremental Ingestion: Captures N days of incremental records via _lastUpdated.
   * Pagination: Recursively follows FHIR next URL links.
   * Raw Storage: Archives raw JSON payloads locally (/tmp/fhir_raw/...).
   * Bronze Enriched: Appends audit metadata (extraction_timestamp, api_url_or_params).

3. Silver Transformation Layer (02_Silver_SCD2_Transformation):
   * Key Normalization: Strips the Patient/ prefix from subject.reference foreign keys (regexp_replace).
   * SCD Type 2: Computes MD5 hash_key on attribute sets, maintaining historical changes via effective_date, end_date, and is_current boolean flags.

4. Gold Analytics Layer (03_Gold_Analytics_Modeling):
   * gold.dim_patient: Conformed patient dimension containing active demographical attributes.
   * gold.fact_patient_encounters: Analytics-ready fact table joining patient records with pre-aggregated encounters, observations, and conditions.

---

## 🔗 Table Relationships & ERD Mapping

       gold.dim_patient
      ┌──────────────────┐
      │ PK: patient_id   │
      │     gender       │
      │     birthDate    │
      └────────┬─────────┘
               │ 1
               │
               │ N
      ┌────────┴────────────────────────┐
      │  gold.fact_patient_encounters   │
      ├─────────────────────────────────┤
      │ FK: patient_id                  │
      │     encounter_ids (Aggregated)  │
      │     condition_ids (Aggregated)  │
      │     observation_ids (Aggregated)│
      └─────────────────────────────────┘

---

## ⚙️ Orchestration Pipeline Setup

The orchestration pipeline is implemented as a Databricks Workflow Job:
* Job Name: FHIR_Medallion_Orchestration_Pipeline
* Trigger Type: Manual / Scheduled Cron
* Compute: Serverless / Shared Cluster

### Task Execution DAG
1. 00_Schema_Creation -> Initializes schemas (raw, bronze, silver, gold) and populates raw.ingestion_metadata.
2. 01_Raw_Bronze_Ingestion -> Dynamically ingests Patient -> Encounter -> Observation -> Condition.
3. 02_Silver_SCD2_Transformation -> Applies key cleaning and SCD Type 2 merges across all 4 entities.
4. 03_Gold_Analytics_Modeling -> Pre-aggregates child clinical entities and populates gold.dim_patient and gold.fact_patient_encounters.

(See workflow_definition.yaml / workflow_definition.json for the complete Databricks Job definition).

---

## 📂 Repository Structure

```text
FHIR_Medallion_Pipeline/
├── 00_Schema_Creation.ipynb
├── 01_Raw_Bronze_Ingestion.ipynb
├── 02_Silver_SCD2_Transformation.ipynb
├── 03_Gold_Analytics_Modeling.ipynb
├── workflow_definition.yaml
└── README.md