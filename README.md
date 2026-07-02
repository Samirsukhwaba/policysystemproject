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
### 1.4.1. ADF pipeline for the Incremental Load Ingestion of Claim Data.
<img width="2930" height="1058" alt="image" src="https://github.com/user-attachments/assets/fbbf8099-b2bc-4c17-a2f4-87fd1b13e3e0" />

### 1.4.2. ADF pipeline for the Incremental Load Ingestion of Agent Data.
<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 24 39 pm" src="https://github.com/user-attachments/assets/56d18752-dc35-40e0-ad10-670ac77f7027" />

### 1.4.3. ADF pipeline for Full Load ingestion of Branch Data.

<img width="1463" height="485" alt="Screenshot 2026-07-02 at 3 25 50 pm" src="https://github.com/user-attachments/assets/83adc201-d402-4f0b-9d42-99043d022167" />

## 1.5. Bronze_Layer Delta Table Creation
####

