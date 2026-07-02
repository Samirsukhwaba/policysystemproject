# 1.End-to-End Data Engineering Pipeline For a S_Insurance Company

This project has created an end-to-end data engineering pipeline for an insurance company to analyse claims data and perform customer segmentation. This will enable the company to better understand its customers’ needs and tailor its offerings accordingly.

## 1.1. Smart Policy Data System- High Level Detail

|Component|	Description|
|-----|-----------|
Source Systems|	 CSV, JSON, SQL Server|
Architecture|Bronze Layer → Silver Layer → Gold Layer|
Data Issues	|Inconsistent data requiring cleaning|
Bronze Layer|	Raw data ingestion|
Silver Layer|	Data cleaning and transformations|
Gold Layer|	Curated Data Lakehouse in Databricks|
Reporting	|Power BI connected to Gold Layer|

## 1.2. Architecture Diagram

<img width="666" height="442" alt="image" src="https://github.com/user-attachments/assets/f1cb90a4-6f40-46bc-805e-58497588bec2" />

## 1.3. Services used in this project

|Services|	Purpose|
|-----|-----------|
Azure Data Lake Storage (ADLS Gen2) |	Store input data (CSV, JSON, SQL, REST API)|
Azure Data Factory (ADF)|	Build and orchestrate data pipelines|
Azure SQL Database	|Source relational data|
Azure Databricks|	Data processing, notebooks, and Lakehouse|
Azure Key Vault	|Securely store secrets and credentials"
Azure DevOps (Git)	|Version control and CI/CD"
Power BI | Reporting and data visualization|


## 1.4. Data Ingestion Using Azure Data Factory into ADLS Gen2
|Data Source	|Source System|	Format|Load Type|Ingestion method|Landing Location|
|--------------|--------------|----------------|----------|-----------------|-----------------|
|Branch Data	|Azure SQL Database|	SQL Tables| Full Load|ADF |ADLS Gen2 |
|Claim Data	|Azure SQL Database	|SQL Tables| Incremental|	ADF |ADLS Gen2 |
|Agent Data	|Azure SQL Database	|SQL Tables	|Incremental|ADF |ADLS Gen2 |
|Policy Data	|Upstream System	|JSON | Incremental|ADF|	ADLS Gen2 |
|Customer Information|	Upstream System	|CSV|Incremental Load(after initial full load)| ADF| ADLS Gen2 |
### 1.4.1. **ADF pipeline for the Incremental Load Ingestion of Claim Data.**

The Claim data ingestion pipeline was implemented in Azure Data Factory (ADF) using a **High Water Mark (HWM)** approach to load only newly inserted or updated records from the source **SQL database** into **Azure Data Lake Storage (ADLS) Gen2.** The pipeline consists of the following steps:

**A.** **Retrieve the Current High Water Mark (HWM)**

* A Lookup activity reads the previously processed timestamp from a CSV/text file stored in ADLS Gen2.
* This timestamp represents the last successful data extraction and serves as the lower boundary for the next incremental load.
  
**B.** **Retrieve the Latest Timestamp from the Source Database**

* A second Lookup activity executes the following SQL query on the source Azure SQL Database:

    - **`select max(lastupdatedtimestamp) as last_update_timestamp from dbo.claim`**

* This value represents the most recent update available in the source table and is used as the upper boundary for the incremental extraction.
  
**C.** **Extract Incremental Data**

* A Copy Data activity retrieves only the records that satisfy the following condition:

    - **`select * from dbo.claim where lastupdatedtimestamp > '@{activity('get hwm from csv').output.firstRow.Prop_0}' and lastupdatedtimestamp         <= '@{activity('get max date from table').output.firstRow.last_update_timestamp }'`**
* This ensures that only newly added or modified records since the previous pipeline execution are extracted.
* The extracted data is written to Azure Data Lake Storage Gen2 in Parquet format under the claim folder.

**D.** **Update the High Water Mark**
* After the data is successfully copied, another Copy Data activity writes the latest timestamp obtained in Step 2 back to the High Water Mark (HWM) file stored in ADLS Gen2.
* This updated timestamp becomes the reference point for the next pipeline execution.
<img width="2930" height="1058" alt="image" src="https://github.com/user-attachments/assets/fbbf8099-b2bc-4c17-a2f4-87fd1b13e3e0" />

### 1.4.2. ADF pipeline for the Incremental Load Ingestion of Agent Data.
<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 24 39 pm" src="https://github.com/user-attachments/assets/56d18752-dc35-40e0-ad10-670ac77f7027" />

### 1.4.3. ADF pipeline for Full Load ingestion of Branch Data.

<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 25 50 pm" src="https://github.com/user-attachments/assets/83adc201-d402-4f0b-9d42-99043d022167" />

### 1.5. Implementation of Medallion Architecture 
#### **1.5.1. Bronze Layer**

The following steps are applied to all data files (Agent, Branch, Claim, Customer, and Policy):
* Mounted **Azure Data Lake Storage (ADLS) Gen2** to **Azure Databricks** by creating an external location in the Databricks Unity Catalog and configuring an Azure Databricks Access Connector for secure data access.
* Read all source files from ADLS Gen2 and added a new column, **`Merge_Flag`**, to identify newly ingested records. This flag is later used in the Silver layer to process only incremental (new) data.
* Stored the ingested data as **Delta tables** in the **Bronze layer schema** using **append mode**
 to preserve the raw historical data.
* After successful ingestion, moved the processed source files to a **processed** folder in ADLS Gen2 to ensure that subsequent pipeline executions process only newly arrived files.

  **Sample Bronze Layer Implementation (Agent Data Ingestion)**

  The following Databricks notebook demonstrates the implementation of the Bronze layer for the Agent dataset. The same approach is used for         the Branch, Claim, Customer, and Policy datasets, with only the source path and target table names changed.

  <img width="1029" height="218" alt="Screenshot 2026-07-02 at 4 03 13 pm" src="https://github.com/user-attachments/assets/eee284b1-326d-4353-927d-3f23410d38f7" />
  
  **Code Explanation:**

  * Reads newly arrived Agent data from the new-data folder in ADLS Gen2.
  * Adds a merge_flag column with the default value False, which is used by the Silver layer to identify records that have not yet been merged.
  * Writes the data to the Bronze Delta table using append mode to retain all ingested records.
  * Moves the successfully processed files from the new-data folder to a date-stamped processed folder, preventing duplicate ingestion during       subsequent pipeline executions.

#### **1.5.2. Silver Layer**

