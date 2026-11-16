# Azure Healthcare Data Engineering | End-to-End Medallion Architecture

An end-to-end Azure Data Engineering portfolio project demonstrating automated ingestion, transformation, orchestration, incremental processing, data quality, auditing, and recovery using Azure Data Factory, Azure Databricks, PySpark, Delta Lake, and ADLS Gen2.

## Project Overview

This project implements a healthcare patient-data processing pipeline using a Bronze, Silver, and Gold Medallion architecture.

The solution processes synthetic patient CSV files from Azure Data Lake Storage, orchestrates Databricks notebooks through Azure Data Factory, maintains file-processing control and audit records, and produces a curated Gold patient dimension.

The project was developed and tested in an Azure environment. The Azure subscription was subsequently canceled to prevent ongoing infrastructure charges.

## Technology Stack

| Category | Technologies |
|---|---|
| Cloud Platform | Microsoft Azure |
| Orchestration | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 |
| Data Processing | Azure Databricks, PySpark, Spark SQL |
| Data Lakehouse | Delta Lake, Medallion Architecture |
| Governance | Databricks Unity Catalog |
| Data Engineering | Incremental Processing, Delta MERGE, Data Validation |
| Monitoring | File Control Tables, Stage Auditing, Recovery Workflows |
| Version Control | Git, GitHub |

## High-Level Architecture

```text
Synthetic Healthcare CSV Files
              |
              v
     ADLS Gen2 - Landing
              |
              v
       Azure Data Factory
     Discovery & Registration
              |
              v
       File Claim & Control
              |
              v
       Landing to Bronze
              |
              v
    Databricks Bronze Ingestion
              |
              v
    Databricks Silver Processing
              |
              v
       Gold Processing
       Backfill / MERGE
              |
              v
     Gold.DimPatientCurrent
              |
              v
    File Audit & Completion
              |
              v
      ADLS Archive Copy
```

## Medallion Architecture

### Bronze Layer
- Ingests raw patient records from source CSV files.
- Preserves source metadata and batch information.
- Performs schema and required-column validation.
- Supports file-level processing controls.

### Silver Layer
- Applies data cleansing and transformation logic.
- Maintains patient dimension records.
- Supports incremental processing and active patient identification.
- Performs data quality and validation checks.

### Gold Layer
- Produces `gold.DimPatientCurrent`.
- Performs an initial backfill of active Silver patients.
- Uses incremental Delta MERGE operations for subsequent batches.
- Maintains Gold processing status and audit records.

## Azure Data Factory Pipelines

### 1. `pl_ingest_healthcare_csv`
Initial ingestion pipeline that copies patient CSV files from Landing to Bronze and invokes the Databricks Bronze ingestion notebook.

### 2. `pl_orchestrate_healthcare_files`
Main orchestration pipeline that automates:

1. Landing file discovery
2. File registration
3. Pending file selection
4. File claim and eligibility checking
5. Landing-to-Bronze copy
6. Bronze ingestion
7. Silver processing
8. Gold processing
9. Archive existence check
10. Processed-file archive copy

### 3. `pl_process_gold`
Executes the Databricks Gold patient dimension notebook.

### 4. `pl_recover_bronze_ingestion`
Validates Bronze recovery eligibility and conditionally invokes Bronze ingestion or a failure activity.

### 5. `pl_recover_silver_ingestion`
Executes the recovery coordinator notebook and invokes the Gold processing pipeline.

### 6. `pl_test_landing_discovery`
Supporting pipeline used during landing file discovery development and testing.

## Data Quality and Audit Framework

The solution includes:

- Required-column validation
- Batch and file metadata tracking
- File claim and processing status management
- Stage-level audit logging
- Recovery eligibility checks
- Incremental processing controls
- Silver-to-Gold record-count reconciliation

Key control objects include:

- `etl.file_control`
- `etl.file_stage_audit`
- `etl.gold_load_control`

## Verified Execution Results

| Validation | Result |
|---|---|
| Batch 7 | Gold initial backfill completed |
| Batch 8 | Incremental Gold processing recovered successfully |
| Batch 9 | End-to-end ADF orchestration succeeded |
| Silver active patient count | 12 |
| Gold current patient count | 12 |
| Reconciliation difference | 0 |
| Batch 9 archive | Confirmed |

**Note:** This project uses synthetic healthcare data. The record counts represent development test data, not production healthcare volumes.

## Engineering Challenges and Solutions

- Resolved Azure storage access and networking configuration issues.
- Implemented parameterized ADF and Databricks workflows.
- Corrected child-pipeline parameter mapping during Gold processing.
- Implemented recovery workflows to support controlled processing.
- Added file control and audit mechanisms for pipeline traceability.
- Validated incremental Gold processing and successful end-to-end orchestration.

## Repository Structure

```text
azure-healthcare_data-engineering/
├── adf/
│   └── pipeline/
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── orchestration/
├── .gitignore
└── README.md
```

## Portfolio Documentation

Detailed architecture diagrams, ADF pipeline screenshots, execution evidence, recovery workflows, and technical project documentation will be added to the repository.

## Project Status

**Completed — November 2026**

The final end-to-end orchestration was successfully executed and validated. The Azure subscription was canceled after project completion to avoid ongoing cloud infrastructure costs.

## Disclaimer

This is an independent portfolio project using synthetic patient data. It is intended to demonstrate Azure Data Engineering concepts and hands-on implementation experience, not to represent a production healthcare deployment.
