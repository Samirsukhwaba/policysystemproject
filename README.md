# End-to-End Azure Data Engineering Pipeline for Insurance Analytics.

This project demonstrates the implementation of an **end-to-end Azure Data Engineering pipeline** for an insurance company using the **Medallion Architecture (Bronze → Silver → Gold)**. The solution ingests data from Azure SQL Database into Azure Data Lake Storage using Azure Data Factory, processes and transforms the data with Azure Databricks and Delta Lake, and produces curated analytical datasets for reporting in Power BI.

The pipeline implements both **full** and **incremental (High Water Mark)** ingestion strategies, performs data validation and cleansing in the Silver layer, and applies business transformations in the Gold layer to generate insights such as claims analysis, sales by policy type, customer segmentation, and agent performance.

## Key Features

- End-to-end Azure Data Engineering pipeline
- Full and Incremental (High Water Mark) data ingestion
- Medallion Architecture (Bronze, Silver, Gold)
- Delta Lake and Unity Catalog implementation
- Data cleansing, validation, and transformation
- Incremental MERGE operations using Delta tables
- Business-ready analytical datasets
- Interactive Power BI dashboards
- Version control using Azure DevOps
---

## Table of Contents
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
- [Future Enhancements](#future-enhancements)
- [Conclusion](#conclusion)

---

# Architecture

The system follows a **Medallion Architecture**:
Bronze → Silver → Gold
Raw Data → Cleaned Data → Business-Ready Data

# Architecture Diagram

<img width="666" height="442" alt="architecture" src="https://github.com/user-attachments/assets/f1cb90a4-6f40-46bc-805e-58497588bec2" />

---

# Smart Policy Data System

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

# Technology Stack

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

# Data Ingestion (ADF Pipelines)

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

## Step 1: Get HWM Date

- Lookup activity reads the last processed timestamp from ADLS.

## step 2: Get Latest Timestamp

```sql
SELECT MAX(lastupdatedtimestamp) AS last_update_timestamp
FROM dbo.claim;
```

## step 3: Extract Incremental Data

```sql
SELECT *
FROM dbo.claim
WHERE lastupdatedtimestamp >
'@{activity('get hwm from csv').output.firstRow.Prop_0}'
AND lastupdatedtimestamp <=
'@{activity('get max date from table').output.firstRow.last_update_timestamp}';
```

## step 4: Update HWM

```sql
SELECT
'@{activity('get max date from table').output.firstRow.last_update_timestamp }'
FROM dbo.claim;
```
- Latest timestamp stored back in ADLS
  
  
## step 5: Claim Data Incremental Pipeline


 <img width="2930" height="1058" alt="image" src="https://github.com/user-attachments/assets/fbbf8099-b2bc-4c17-a2f4-87fd1b13e3e0" />

 ---

 ## Agent Data (Incremental - HWM)

The Agent pipeline uses a **High Water Mark (HWM)** approach.


## step 1: Get HWM Date

- Lookup activity reads the last processed timestamp from ADLS.

## step 2: Get Latest Timestamp

```sql
SELECT MAX(lastupdatedtimestamp) AS last_update_timestamp
FROM dbo.agent;
```

## step 3: Extract Incremental Data

```sql
SELECT *
FROM dbo.agent
WHERE lastupdatedtimestamp >
'@{activity('get hwm from csv').output.firstRow.Prop_0}'
AND lastupdatedtimestamp <=
'@{activity('get max date from table').output.firstRow.last_update_timestamp}';
```

## step 4: Update HWM

```sql
SELECT
'@{activity('get max date from table').output.firstRow.last_update_timestamp }'
FROM dbo.agent;
```
- Latest timestamp stored back in ADLS
  
  
## step 5: Agent Data Incremental Pipeline

<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 24 39 pm" src="https://github.com/user-attachments/assets/56d18752-dc35-40e0-ad10-670ac77f7027" />

---

## Branch Data (Full Load)
 
 The Branch table is a reference dataset, so a full load approach is used.

## Step 1: Extract Data

- Copy entire dbo.branch table from Azure SQL Database.

## step 2: Load into ADLS Gen2

- Stored in Parquet format under /branch.

## step 3: Branch Data Pipeline

<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 25 50 pm" src="https://github.com/user-attachments/assets/83adc201-d402-4f0b-9d42-99043d022167" />

--- 

# Medallion Architecture

---

## Bronze Layer

The **Bronze layer** is the raw data ingestion layer of the Medallion Architecture. It ingests data from Azure Data Lake Storage (ADLS) Gen2 into Delta tables while preserving the original source data for downstream processing.

### Responsibilities

- Read source data from **ADLS Gen2** using **Unity Catalog External Locations** and the **Azure Databricks Access Connector**.
- Add a **`merge_flag`** column with a default value of **`False`** to support incremental processing in the Silver layer.
- Store the ingested data as **Delta tables** in the Bronze Layer schema.
- Write data in **append** mode to preserve historical records.
- Move successfully processed source files to the **processed** folder to prevent duplicate ingestion during subsequent pipeline executions.

---

## Example: Agent Data Ingestion into the Bronze Layer

The following notebook demonstrates the implementation of the Bronze layer for the **Agent** dataset. The same ingestion pattern was followed for the **Branch**, **Claim**, **Customer**, and **Policy** datasets.

```python
# agent_data_ingestion_to_bronze

from datetime import datetime
from pyspark.sql.functions import lit

# Read new agent data from ADLS Gen2
agent_df = spark.read.parquet(
    "/Volumes/policysystemadb/default/policysystemlocationvol/agent/new-data/"
)

# Add merge flag for incremental processing
agent_df_with_flag = agent_df.withColumn("merge_flag", lit(False))

# Write data to the Bronze Delta table
agent_df_with_flag.write.mode("append").saveAsTable("bronze_layer.agent")

# Move processed files to the processed folder
dbutils.fs.mv(
    "/Volumes/policysystemadb/default/policysystemlocationvol/agent/new-data/",
    "/Volumes/policysystemadb/default/policysystemlocationvol/agent/processed/" +
    datetime.now().strftime("%m-%d-%Y"),
    True
)
```

> **Note:** The same implementation approach was used for the **Branch**, **Claim**, **Customer**, and **Policy** datasets, with only the source file paths and target Bronze Delta tables varying for each entity.

---

# Silver Layer

The **Silver layer** is responsible for cleansing, validating, and transforming the raw data ingested into the Bronze layer. At this stage, data quality is improved by applying validation rules, removing invalid records, and performing incremental processing before storing the curated data as Delta tables. These datasets serve as the foundation for downstream analytics and reporting.

## Responsibilities

- Create Delta tables with predefined schema for each business entity.
- Remove duplicate and invalid records.
- Handle missing, empty, and null values.
- Apply data quality and business validation rules.
- Standardize data formats and schemas.
- Perform incremental processing using the **`merge_flag`**.
- Merge new and updated records into the Silver Delta tables.
- Produce clean, analytics-ready datasets.

---

# Example: Customer Data Processing

The following notebook demonstrates the implementation of the Silver layer for the **Customer** dataset.

## Step 1: Read and Validate Bronze Data

Customer data is read from the Bronze layer, and only records with **`merge_flag = False`** are selected for processing.

The following validation rules are applied:

- Remove records with a null **customer_id**.
- Retain only customers whose **gender** is **Male** or **Female**.
- Ensure **registration_date** is later than **date_of_birth**.

```python
from pyspark.sql.functions import *

customer_df = spark.read.table("bronze_layer.customer") \
    .where(
        (col("customer_id").isNotNull()) &
        (col("merge_flag") == False)
    ) \
    .filter(col("gender").isin("Male", "Female")) \
    .where(col("registration_date") > col("date_of_birth"))
```

---

## Step 2: Create a Temporary View

Create a temporary view to use as the source for the merge operation.

```python
customer_df.createOrReplaceTempView("clean_result")
```

---

## Step 3: Merge into the Silver Layer

Merge the validated records into the Customer Delta table.

- Existing records are **updated**.
- New records are **inserted**.

```python
spark.sql("""
MERGE INTO silver_layer.customer t
USING clean_result s
ON t.customer_id = s.customer_id

WHEN MATCHED THEN
UPDATE SET
    t.first_name = s.first_name,
    t.last_name = s.last_name,
    t.email = s.email,
    t.phone = s.phone,
    t.country = s.country,
    t.city = s.city,
    t.registration_date = s.registration_date,
    t.date_of_birth = s.date_of_birth,
    t.gender = s.gender,
    t.merged_timestamp = current_timestamp()

WHEN NOT MATCHED THEN
INSERT (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    country,
    city,
    registration_date,
    date_of_birth,
    gender,
    merged_timestamp
)
VALUES (
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.phone,
    s.country,
    s.city,
    s.registration_date,
    s.date_of_birth,
    s.gender,
    current_timestamp()
)
""")
```

---

## Step 4: Update the Merge Flag

Once the merge operation completes successfully, update the **`merge_flag`** in the Bronze table so that the same records are not processed again.

```sql
UPDATE bronze_layer.customer
SET merge_flag = TRUE
WHERE merge_flag = FALSE;
```

> **Note:** The same implementation approach was followed for the **Agent**, **Branch**, **Claim**, and **Policy** datasets. Each entity underwent data cleaning, validation, duplicate removal, null value handling, data type and timestamp-to-date conversions, and other entity-specific transformations before being incrementally merged into the corresponding Silver Delta table.


This approach provides a consistent incremental processing framework while ensuring that each dataset is cleansed and validated according to its domain-specific requirements.

---
# Gold Layer


The **Gold layer** contains business-ready, aggregated datasets designed for reporting, dashboarding, and analytics. Data from the Silver layer is transformed according to business requirements and stored as curated Delta tables.

## Responsibilities

- Create Delta tables with predefined schemas for analytical datasets.
- Read validated data from the Silver layer.
- Apply business logic and analytical transformations.
- Aggregate and summarize data based on business requirements.
- Join multiple Silver layer entities to generate business insights.
- Store the processed results in Gold Delta tables using **MERGE** operations to support incremental updates.

---

# Example 1: Claims Analysis by Policy Type

The following notebook generates claim statistics by joining the **Claim** and **Policy** datasets.

## Business Requirement

Analyse claims by **policy type** to determine:

- Claim status
- Average claim amount
- Maximum claim amount
- Minimum claim amount
- Total number of claims

```python
claim_df = spark.sql("""
SELECT
    p.policy_type,
    any_value(c.claim_status) AS claim_status,
    AVG(claim_amount) AS avg_claim_amount,
    MAX(claim_amount) AS max_claim_amount,
    MIN(claim_amount) AS min_claim_amount,
    COUNT(*) AS total_claim
FROM silver_layer.claim c
JOIN silver_layer.policy p
ON c.policy_id = p.policy_id
GROUP BY policy_type
HAVING policy_type IS NOT NULL
""")

display(claim_df)
```

## Output: Claim Summary Statistics by Policy Type


<img width="2060" height="606" alt="image" src="https://github.com/user-attachments/assets/6e1b338c-7799-42e0-947d-e6597f33a0db" />


---

# Example 2: Sales by Policy Type and Month

## Business Requirement

Calculate the total premium collected for each **policy type** by **sale month**.

### Step 1: Aggregate Sales Data

```python
sales_df = spark.sql("""
SELECT
    policy_type,
    TO_DATE(start_date) AS sale_month,
    SUM(premium) AS total_premium
FROM silver_layer.policy
GROUP BY
    policy_type,
    sale_month
""")

sales_df.createOrReplaceTempView("gold_sales")
```

### Step 2: Merge into Gold Table

```python
spark.sql("""
MERGE INTO gold_layer.sales_by_policy_type_and_month t
USING gold_sales s
ON t.policy_type = s.policy_type
AND t.sale_month = s.sale_month

WHEN MATCHED THEN
UPDATE SET
    t.total_premium = s.total_premium,
    t.updated_timestamp = current_timestamp()

WHEN NOT MATCHED THEN
INSERT (
    policy_type,
    sale_month,
    total_premium,
    updated_timestamp
)
VALUES (
    s.policy_type,
    s.sale_month,
    s.total_premium,
    current_timestamp()
)
""")
```

---
## Output: Sales by Policy Type and Month

<img width="2044" height="1428" alt="image" src="https://github.com/user-attachments/assets/0aefefaf-94af-41d8-a7a0-bf37759c3424" />

---

# Example 3: Claims by Policy Type and Claim Status

## Business Requirement

Generate a summary showing the number of claims and total claim amount for each **policy type** and **claim status**.

### Step 1: Aggregate Claim Data

```python
claim_df = spark.sql("""
SELECT
    p.policy_type,
    c.claim_status,
    COUNT(*) AS total_claim,
    SUM(claim_amount) AS total_claim_amount
FROM silver_layer.claim c
JOIN silver_layer.policy p
ON c.policy_id = p.policy_id
GROUP BY
    policy_type,
    claim_status
HAVING
    p.policy_type IS NOT NULL
ORDER BY
    policy_type
""")

claim_df.createOrReplaceTempView("gold_claim")
```

### Step 2: Merge into Gold Table

```python
spark.sql("""
MERGE INTO gold_layer.claim_by_policy_type_and_status t
USING gold_claim s
ON t.policy_type = s.policy_type

WHEN MATCHED THEN
UPDATE SET
    t.claim_status = s.claim_status,
    t.total_claim = s.total_claim,
    t.total_claim_amount = s.total_claim_amount,
    t.updated_timestamp = current_timestamp()

WHEN NOT MATCHED THEN
INSERT (
    policy_type,
    claim_status,
    total_claim,
    total_claim_amount,
    updated_timestamp
)
VALUES (
    s.policy_type,
    s.claim_status,
    s.total_claim,
    s.total_claim_amount,
    current_timestamp()
)
""")
```
---

## Output: Claims by Policy Type and Status

<img width="2046" height="1074" alt="image" src="https://github.com/user-attachments/assets/e84a1a7b-803a-4896-83ef-1de15b2fcae1" />

---
> **Note:** In this project, the Gold layer focuses on **claim analytics**. Business transformations were implemented to generate insights such as **claims by policy type**, **sales by policy type and month**, and **claims by policy type and claim status**. The same approach can be extended to create additional business-oriented analytical datasets as required.

---

# Reporting

The reporting layer is implemented using **Power BI**, which is directly connected to the **Gold layer in Databricks** to enable real-time analytics and business reporting.

## Integration Details

- Power BI is connected to Databricks using the **server hostname** and **HTTP path** of the Databricks cluster.
- This connection allows Power BI to access curated Gold layer Delta tables for reporting and visualization.
- Data is refreshed directly from Databricks, ensuring up-to-date insights for business users.

## Dashboards Developed

The following dashboards were created to support business decision-making:

- Claims Analysis Dashboard
- Customer Segmentation Dashboard
- Agent Performance Dashboard
- Policy Insights Dashboard

## Output: Claim Analysis Dashboard


<img width="1009" height="633" alt="image" src="https://github.com/user-attachments/assets/4741ea8b-e1a7-4f7a-be76-2a8f33a7b3c9" />

---

# Future Enhancements

- Implement workflow orchestration with Databricks Workflows.
- Add data quality monitoring and alerting.
- Optimize Delta tables using partitioning and Z-Ordering.
- Integrate additional data sources and analytical use cases.

# Conclusion

This project demonstrates the implementation of an end-to-end Azure Data Engineering solution for an insurance analytics use case using the **Medallion Architecture (Bronze → Silver → Gold)**. The pipeline combines Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, Unity Catalog, Azure Key Vault, Azure DevOps, and Power BI to build a scalable and reliable data platform.

The solution supports both **full** and **incremental (High Water Mark)** data ingestion, performs data cleansing and validation in the Silver layer, and applies business transformations in the Gold layer to produce analytics-ready datasets. These curated datasets enable meaningful business insights through interactive Power BI dashboards, including claims analysis, customer segmentation, agent performance, and policy insights.

This project demonstrates key data engineering concepts such as data ingestion, ETL pipeline development, Delta Lake operations, incremental processing, data quality management, Medallion Architecture implementation, and end-to-end reporting. The same architecture can be extended to support additional data sources, analytical use cases, and enterprise-scale workloads, making it a strong foundation for modern cloud-based data platforms.


