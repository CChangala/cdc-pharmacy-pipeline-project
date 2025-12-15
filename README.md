# 💊 Pharmaceutical Utilization ELT Pipeline (BigQuery & dbt)

## 🎯 Project Overview
This project establishes a complete **ELT (Extract, Load, Transform)** pipeline designed to ingest raw Medicare Part D utilization data, clean it, and model it into a consumer-ready Star Schema data mart using **dbt (Data Build Tool)** and **Google BigQuery**.

The final output is an analytical mart (`agg_quarterly_state_utilization`) ready to feed a business intelligence dashboard (e.g., Looker Studio) for analyzing reimbursement trends across US states and quarters.

## 🛠️ Technology Stack

| Category | Tool | Purpose |
| :--- | :--- | :--- |
| **Orchestration/Scripting** | Python | Handles the initial Extract & Load (E/L) process. |
| **Data Warehouse** | Google BigQuery | The target destination for raw, staging, and final modeled data. |
| **Transformation (T)** | dbt (Data Build Tool) | Manages SQL transformations, dependency graph, testing, and documentation.  |
| **Storage** | Google Cloud Storage (GCS) | **External storage** for the large raw data file, decoupling data from the Git repository. |
| **Code Management** | Git/GitHub | Version control for all dbt and Python files. |

## 📐 Data Model (Star Schema)
The project utilizes a Star Schema to structure the data for analytical querying. 



| Model Name | Type | Materialization | Purpose |
| :--- | :--- | :--- | :--- |
| `stg_cms_utilization` | Staging | View | Cleanses and standardizes the raw data (e.g., casting data types, handling nulls). |
| `dim_states` | Dimension | View | Provides a lookup of state codes (`CA`) to full state names (`California`) and includes territories (PR, VI, DC). |
| `fact_utilization` | Fact | Table | Joins cleaned staging data with dimensions to create a primary analytical table for utilization metrics. |
| `agg_quarterly_state_utilization` | Mart | Table | Aggregates the fact data by quarter and state for optimized dashboard performance. |

## ⚙️ Installation & Setup Guide

### Prerequisites
1.  Python (3.8+)
2.  Google Cloud SDK installed and authenticated (`gcloud auth application-default login`).
3.  A Google Cloud Project with the BigQuery API enabled.

### 1. Repository & Data Setup

```bash
# Clone the repository
git clone [YOUR_REPO_URL] cdc-pharmacy-pipeline-project
cd cdc-pharmacy-pipeline-project

# Upload your raw data file (sdud-2025-updated-dec2025.csv) to a GCS bucket.
# The Python script is configured to read from this GCS path.
