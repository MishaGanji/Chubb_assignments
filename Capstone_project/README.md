# Global Sales & Retail Performance Analytics System
## Project Overview

This project delivers an end-to-end retail sales analytics system that integrates data engineering, workflow orchestration, and business intelligence to analyze sales performance, customer behavior, product profitability, and regional trends.

The system follows a **Medallion Architecture (Bronze → Silver → Gold)** to transform raw retail data into analytics-ready datasets and interactive dashboards for business stakeholders.

### Business Context

Retail organizations generate large volumes of transactional data across multiple regions, products, and customer segments. Without a centralized analytics pipeline, this data becomes difficult to process, validate, and analyze consistently.

This project demonstrates how modern data engineering tools can be used to automate data processing, improve data quality, and generate actionable business insights.

### Datasets Used

The project is built using the following datasets:

####  1. sales_transactions.csv 
- Contains detailed order-level sales information including orders, quantities, pricing, discounts, and revenue.

#### 2. products.csv  
- Stores product master data such as product names, categories, sub-categories, and cost details.

#### 3. customers.csv 
- Contains customer master information including customer identifiers, segments, and demographic attributes.

#### 4. stores.csv 
- Holds store-level details including store identifiers, locations, and regional mappings.

#### 5. returns.csv 
- Captures returned order information along with refund amounts and return reasons.

#### 6. sales_targets.csv 
- Defines sales targets by region and time period for performance comparison against actual sales.

![Alt text for the image](path/to/your/image.png)

## Data Processing & ETL Layers
**Bronze Layer – Raw Data Ingestion**

- Reads raw CSV files from Databricks Volumes

- Preserves original schema and values

- Stores data as Delta tables

- No transformations applied

**Silver Layer – Data Cleaning & Validation**

Key preprocessing steps include:

- Duplicate record removal

- Handling missing and invalid values

- Foreign key validation across datasets

- Standardization of date and category formats

- Creation of derived metrics

- Separation of invalid records into dedicated tables

**Gold Layer – Analytics & Aggregation**

The Gold layer contains business-ready analytics tables:

- Sales Performance Analytics

- Product Performance & Profitability

- Regional & Store-Level Performance

- Customer Purchase Analytics

- Target vs Actual Performance

![Alt text for the image](path/to/your/image.png)

## Architecture

                 ┌────────────────────────────┐
                 │        Raw Source Data     │
                 │      (CSV Files Upload)    │
                 │  Sales, Products, Customers│
                 │  Stores, Returns, Targets  │
                 └──────────────┬─────────────┘
                                │
                                ▼
        ┌────────────────────────────────────────────┐
        │               BRONZE LAYER                 │
        │--------------------------------------------│
        │ • Raw data                                 │
        └──────────────────────┬─────────────────────┘
                               │
                               ▼
        ┌───────────────────────────────────────────┐
        │               SILVER LAYER                │
        │-------------------------------------------│
        │ • Clean and Validated Data                │
        └─────────────────────┬─────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────────┐
        │                GOLD LAYER                 │
        │-------------------------------------------│
        │ • Business-level aggregations and KPIs    │   
        └───────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────┐
        │             POWER BI DASHBOARDS           │
        │-------------------------------------------│
        │ • Executive Sales Overview                │
        │ • Regional & Store Performance            │
        │ • Product & Customer Analytics            │
        └───────────────────────────────────────────┘

                    ▲
                    │
        ┌───────────────────────────────────────────┐
        │          APACHE AIRFLOW (ORCHESTRATION)   │
        │-------------------------------------------│
        │ • Controls ETL execution flow             │
        │ • Bronze → Silver → Gold dependencies     │
        │ • Retry logic and execution logging       │
        └───────────────────────────────────────────┘

## Technology Stack

- **Data Processing:** Python, Pandas 
- **Big Data & Analytics:** Azure Databricks, PySpark 
- **Storage:** CSV files, Delta Tables  
- **Workflow Orchestration:** Apache Airflow  
- **Visualization:** Power BI Desktop  


## Project Structure & Execution Workflow

### Project Structure

* **datasets/**
  Raw input retail datasets (CSV files)

* **databricks/**
  Databricks notebooks organized by Medallion Architecture:

  * **01_bronze_ingestion/**
  * **02_silver_transformation/**
  * **03_gold_analytics/**

* **airflow/**
  Apache Airflow setup and DAGs:

  * `Dockerfile`
  * `docker-compose.yaml`
  * `dags/retail_sales_databricks_dag.py`

* **powerbi/**
  Power BI dashboard files (`.pbix`)

* **README.md**
  Project documentation

* **presentation/**
  Final project presentation files

## Execution Process 

### 1. Upload Input Data to Databricks Volumes

#### 1.1 Upload Raw CSV Files

Before running any Databricks notebooks, the raw retail CSV files must be uploaded to Databricks storage.

Steps:
#### 1. Navigate to **Databricks → Catalog → Volumes**
#### 2. Select an existing catalog and schema, or create one
#### 3. Create the following folder structure:
```
/Volumes/misha_azure/default/global_sales/data/
```

#### 4. Upload all raw CSV files from the local `datasets/` folder into this volume path

![Alt text for the image](path/to/your/image.png)

#### 1.2 Update File Paths in Notebooks

All Databricks notebooks are configured to read input data from the Databricks volume path.

Example:

```python
BASE_PATH = "/Volumes/misha_azure/default/global_sales/data/sales_transactions.csv"
```
### 2. Databricks Job Setup

#### 2.1 Import Analytics Notebooks

Import the Databricks notebooks into your workspace.

#### 2.2 Create Databricks Job

#### 1. Go to **Databricks > Jobs & Pipelines > Create Job**
#### 2. Add three tasks in order (Bronze > Silver > Gold) using notebook paths from your workspace.
#### 3. Attach a cluster
#### 4. Save the job and **note the Job ID**

![Alt text for the image](path/to/your/image.png)

### 3. Airflow Setup Using Docker

#### 3.1 Navigate to Airflow Project Directory

```bash
cd airflow
```
#### 3.2 Configure Airflow Docker Environment

The Databricks provider is installed using Docker environment variables.
Ensure the following configuration is present in docker-compose.yaml:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
```
This enables Airflow to communicate with Databricks Jobs.

#### 3.3 Start Airflow Services
```bash
docker compose up -d
``` 

This command starts all required Airflow components

#### 3.4 Access Airflow UI
Open the Airflow web interface in a browser:

```arduino
http://localhost:8080
```
Login using the default credentials configured in Docker:

**Username**: airflow

**Password**: airflow

![Alt text for the image](path/to/your/image.png)

####  3.5 Configure Databricks Connection in Airflow
To allow Airflow to trigger Databricks jobs:

- Navigate to Admin → Connections

- Click Add Connection

- Configure the connection as follows:

```yaml
Conn Id: databricks_default
Conn Type: Databricks
Host: https://adb-<workspace-id>.<region>.azuredatabricks.net
Password: <Databricks Personal Access Token>
```

This connection is used by Airflow DAGs to securely invoke Databricks Jobs.

![Alt text for the image](path/to/your/image.png)

#### 3.6 Create Airflow DAG for ETL Orchestration
An Airflow DAG file is created to define the ETL workflow and task dependencies.

The DAG file is placed inside the dags/ directory.

The DAG code defines:

- Pipeline structure

- Task dependencies

- Databricks job triggers

- Retry and failure handling

DAG File Location:

```bash
dags/retail_sales_databricks_dag.py
```
DAG Responsibilities:

- Orchestrates Bronze → Silver → Gold execution order

- Triggers separate Databricks jobs for each layer

- Ensures reliable and repeatable pipeline execution

#### 3.7 Deploy and Trigger the DAG
Once the DAG file is added, Airflow automatically detects it.
If required, containers can be restarted to refresh the environment.

```bash
docker compose up -d 
```

The DAG named **retail_sales_end_to_end_pipeline** appears in the Airflow UI.

The pipeline should be triggered and run

Execution order enforced by the DAG:

```powershell
Start → Bronze → Silver → Gold → End
```
#### 3.8 Verify Pipeline Execution

Monitor execution using:

- Graph View

- Task Logs

Successful execution confirms:

- Correct task orchestration

- Successful Databricks job invocation

- End-to-end ETL automation

![Alt text for the image](path/to/your/image.png)

### 4. Gold Layer Consumption & Power BI Dashboard Creation

#### 4.1 Create Gold Tables in Databricks

The final analytics tables are created in the **Gold layer** as Delta tables within the Databricks Catalog.  
These tables contain business-ready KPIs and aggregated metrics optimized for reporting and visualization.

Once the Gold tables are successfully created, they are registered in the **Catalog → Tables** section.

---

#### 4.2 Open Gold Tables in Power BI

Databricks provides native integration with Power BI.

Steps:

1. Navigate to **Databricks → Catalog → Tables**
2. Select the required **Gold table**
3. Click **Open in Power BI**
4. Databricks generates a Power BI dataset connection automatically

This enables direct consumption of Gold layer tables in Power BI without manual data export.

![Alt text for the image](path/to/your/image.png)

---

#### 4.3 Authenticate Power BI with Microsoft Azure Account

When prompted by Power BI:

1. Sign in using your **Microsoft Azure account**
2. Grant the required permissions to access Databricks data
3. Establish a secure live connection between Databricks and Power BI

Once authenticated, the Gold tables become available for report creation.

---

#### 4.4 Create Power BI Dashboards

Using the connected Gold layer datasets, three interactive dashboards are created:

- **Sales Performance Dashboard**  
  Overall revenue, profit, sales trends, and KPI metrics

![Alt text for the image](path/to/your/image.png)


- **Regional & Store Performance Dashboard**  
  Region-wise and store-level comparisons with geographic insights

![Alt text for the image](path/to/your/image.png)

- **Product & Customer Analytics Dashboard**  
  Product profitability, customer segmentation, and purchase behavior analysis

![Alt text for the image](path/to/your/image.png)

These dashboards provide actionable insights for business stakeholders and support data-driven decision-making.

## Testing & Data Validation

The pipeline includes validation checks at each stage to ensure data quality:

- Record count reconciliation between Bronze, Silver, and Gold layers
- Null, duplicate, and invalid value checks in the Silver layer
- Referential integrity validation across sales, products, customers, and stores
- Verification of KPI calculations in the Gold layer
- Cross-validation of Power BI dashboard metrics against Gold tables


## Results & Conclusion

* Clear business insights were identified from the retail sales data, including:
  * Sales trends across regions and time periods
  * High-performing and underperforming products and categories
  * Variations in store-level and regional performance
  * Customer purchase patterns and segmentation

* Profitability analysis highlighted:
  * Regions and products contributing the highest profit
  * Impact of returns and discounts on net revenue
  * Differences between actual sales and defined sales targets

### Key Outcomes

* Successfully transformed raw, inconsistent retail datasets into clean, analytics-ready data layers
* Implemented a scalable Medallion Architecture (Bronze, Silver, Gold)
* Created reliable business KPIs such as revenue, profit, and target achievement
* Automated ETL workflows using Apache Airflow with proper task dependencies and logging
* Delivered interactive and insightful Power BI dashboards for business stakeholders
