# End-to-End Data Engineering Pipeline for Insurance Company

This project implements a **scalable end-to-end data engineering pipeline** for an insurance company to analyse claims data and perform customer segmentation.  
The solution is built using **Azure Data Engineering services** and follows the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Smart Policy Data System](#smart-policy-data-system)
- [Services Used](#services-used)
- [Data Ingestion (ADF Pipelines)](#data-ingestion-adf-pipelines)
  - [Full Load vs Incremental Load](#full-load-vs-incremental-load)
  - [Claim Data (Incremental - HWM)](#claim-data-incremental---hwm)
  - [Agent Data (Incremental - HWM)](#agent-data-incremental---hwm)
  - [Branch Data (Full Load)](#branch-data-full-load)
- [Medallion Architecture](#medallion-architecture)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [Reporting](#reporting)
- [Conclusion](#conclusion)

---

## Project Overview

This project builds an **end-to-end data pipeline for an insurance company** to:
- Analyse claims data
- Perform customer segmentation
- Enable business insights for decision-making

The pipeline ensures **data consistency, scalability, and reliability** using Azure services.

---

## Architecture

The system follows a **Medallion Architecture**:
Bronze → Silver → Gold
Raw Data → Cleaned Data → Business-Ready Data

### Architecture Diagram

<img width="666" height="442" alt="architecture" src="https://github.com/user-attachments/assets/f1cb90a4-6f40-46bc-805e-58497588bec2" />

---

## Smart Policy Data System

| Component | Description |
|----------|-------------|
| Source Systems | CSV, JSON, SQL Server |
| Architecture | Bronze → Silver → Gold |
| Data Issues | Inconsistent raw data requiring cleaning |
| Bronze Layer | Raw ingestion layer |
| Silver Layer | Data cleaning & transformation |
| Gold Layer | Curated analytics layer |
| Reporting | Power BI dashboards |

---

## Services Used

| Service | Purpose |
|---------|--------|
| Azure Data Lake Storage (ADLS Gen2) | Data storage (raw + processed) |
| Azure Data Factory (ADF) | Data ingestion & orchestration |
| Azure SQL Database | Source system |
| Azure Databricks | Data processing (Lakehouse) |
| Azure Key Vault | Secret management |
| Azure DevOps (Git) | CI/CD and version control |
| Power BI | Reporting & visualization |

---

## Data Ingestion (ADF Pipelines)

### Full Load vs Incremental Load

| Data Source | Load Type | Method |
|------------|----------|--------|
| Branch | Full Load | ADF Copy Activity |
| Claim | Incremental | High Water Mark (HWM), ADF Copy Activity |
| Agent | Incremental | High Water Mark (HWM), ADF Copy Activity |
| Policy | Incremental | Upstream |
| Customer | Incremental | Upstream |

---

## Claim Data (Incremental - HWM)

The Claim pipeline uses a **High Water Mark (HWM)** approach.
### Steps

**1. Get HWM Date**

- Lookup activity reads the last processed timestamp from ADLS.

**2. Get Latest Timestamp**

```sql
SELECT MAX(lastupdatedtimestamp) AS last_update_timestamp
FROM dbo.claim;
```

**3. Extract Incremental Data**

```sql
SELECT *
FROM dbo.claim
WHERE lastupdatedtimestamp >
'@{activity('get hwm from csv').output.firstRow.Prop_0}'
AND lastupdatedtimestamp <=
'@{activity('get max date from table').output.firstRow.last_update_timestamp}';
```

**4. Update HWM**

```sql
SELECT
'@{activity('get max date from table').output.firstRow.last_update_timestamp }'
FROM dbo.claim;
```
- Latest timestamp stored back in ADLS
  
  
**5. Claim Data Pipeline**


 <img width="2930" height="1058" alt="image" src="https://github.com/user-attachments/assets/fbbf8099-b2bc-4c17-a2f4-87fd1b13e3e0" />

 ## Agent Data (Incremental - HWM)

The Agent pipeline uses a **High Water Mark (HWM)** approach.
### Steps

**1. Get HWM Date**

- Lookup activity reads the last processed timestamp from ADLS.

**2. Get Latest Timestamp**

```sql
SELECT MAX(lastupdatedtimestamp) AS last_update_timestamp
FROM dbo.agent;
```

**3. Extract Incremental Data**

```sql
SELECT *
FROM dbo.agent
WHERE lastupdatedtimestamp >
'@{activity('get hwm from csv').output.firstRow.Prop_0}'
AND lastupdatedtimestamp <=
'@{activity('get max date from table').output.firstRow.last_update_timestamp}';
```

**4. Update HWM**

```sql
SELECT
'@{activity('get max date from table').output.firstRow.last_update_timestamp }'
FROM dbo.agent;
```
- Latest timestamp stored back in ADLS
  
  
**5. Agent Data Pipeline**

<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 24 39 pm" src="https://github.com/user-attachments/assets/56d18752-dc35-40e0-ad10-670ac77f7027" />

 ## Branch Data (Full Load)
 
 The Branch table is a reference dataset, so a full load approach is used.

### Steps

**1. Extract Data**

- Copy entire dbo.branch table from Azure SQL Database.

**2. Load into ADLS Gen2**

- Stored in Parquet format under /branch.

**3. Branch Data Pipeline**

<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 25 50 pm" src="https://github.com/user-attachments/assets/83adc201-d402-4f0b-9d42-99043d022167" />

## Medallion Architecture

### Bronze Layer
Raw ingestion layer.

Responsibilities:

* Read data from ADLS Gen2
* Add Merge_Flag = False
* Store data as Delta tables
* Append mode storage
* Move processed files to archive folder
  
**Example of Bronze Layer Implementation**

<img width="1029" height="218" alt="Screenshot 2026-07-02 at 4 03 13 pm" src="https://github.com/user-attachments/assets/eee284b1-326d-4353-927d-3f23410d38f7" />

### Silver Layer

The Silver layer performs data cleaning and transformation.

Responsibilities:

* Remove duplicates
* Handle missing values
* Standardise formats and schemas
* Apply business rules
* Join multiple datasets (Claim, Policy, Customer, Agent, Branch)
* Convert raw Bronze data into analytics-ready datasets

Output:

* Cleaned and structured Delta tables

### Gold Layer

The Gold layer contains business-ready datasets.

* Star schema design
* Aggregated tables
* Used for analytics and reporting


### Reporting

* Power BI connected to Gold Layer
* Dashboards for:
    * Claims analysis
    * Customer segmentation
    * Agent performance
    * Policy insights
