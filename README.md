# **DESIGNING AN ELT PIPELINE WITH SCD STRATEGY IMPLEMENTATION IN A DATA WAREHOUSE**

This project focuses on building a robust Extract, Load, and Transform (ELT) pipeline for efficient data integration within a data warehouse environment. The pipeline incorporates Slowly Changing Dimension (SCD) strategies to manage and track changes in dimensional data over time, ensuring historical accuracy and consistency.
You need to read this [article](https://medium.com/@ibnufajar_1994/building-robust-data-warehouses-using-the-kimball-approach-fa95481018f3)  before you use this repository to get basic understanding of the datawarehouse design from this repository.

## **Requirement Gathering**
 Imagine you are a data engineer, and your client Olist, the largest e-commerce company in Brazil, asks you to apply SCD strategies to their data warehouse. You will conduct discussion with client and ask some important question that will guide you to determine which SCD strategies that suite for the data warehouse that they have.

**QUESTION 1:** Which dimensions do you think are most critical to track historical changes?

**POSSIBLE ANSWER:** I believe the Product, Customer, and Seller dimensions are the most important for us to track changes over time.


**QUESTION 2:** How often do you expect these dimensions to change, and how quickly do you need to see these changes reflected in your reports?
**POSSIBLE ANSWER:** Product information might change weekly, while customer and seller information could change monthly. We'd like to see these changes in our reports within 24 hours of the change occurring.


**QUESTION 3:** For these dimensions, do you need to keep a full history of all changes, or is it sufficient to know the current state and the previous state?

**POSSIBLE ANSWER:** For products, customers, and selers  we need a full history as it's crucial for our product evolution analysis. For other dimension, knowing the current and previous state should be sufficient.


**QUESTION 4:** Are there any specific attributes within these dimensions that are more important to track historically than others?

**POSSIBLE ANSWER:** Yes, for products, we're particularly interested in price changes and category changes. For customers and sellers, address changes are the most important.

**QUESTION 4:**  How long do you need to retain historical data for these slowly changing dimensions?

**POSSIBLE ANSWER:** We'd like to keep product history indefinitely. For customers and sellers, retaining history for the past 3 years should be sufficient.

## DETERMINE THE SCD STRATEGY

Based on the information gathered from the client, we can determine the appropriate Slowly Changing Dimension (SCD) strategies for the Olist data warehouse model. Let's break it down by dimension:

**Product Dimension**
- SCD Type: Type 2
- Reason: The client needs a full history of all changes, especially for product & category names, also description changes. They want to keep this history indefinitely.
- Implementation: We'll add "created_date", "expired_date", and "current_flag" columns to the dim_product table. When a product's attributes change, we'll insert a new row with the updated information, set the "expired_date" of the previous row, and update the "current_flag" flags accordingly.

**Customer Dimension**
SCD Type: Type 2 for address changes, Type 1 for other attributes
- Reason: The client specifically mentioned address changes as important to track historically, but only needs current and previous states. Other attributes can be overwritten.
- Implementation: We'll add "created_date", "expired_date", and "current_flag" columns to the dim_customer table. For address changes, we'll insert a new row and update the dates and flags. For other attribute changes, we'll simply update the current row.

**Seller Dimension:**
- SCD Type: Type 2 for address changes, Type 1 for other attributes
- Reason Similar to the customer dimension, address changes are important to track historically, but only current and previous states are needed.
- Implementation: We'll add "create_date", "expired_date", and "current_flag" columns to the dim_seller table. The implementation will be similar to the customer dimension.

**Payments Dimension**
- SCD Type: 1
- Reason: We only need to focus on current and uptodate payment information of the customer. The SCD type 1 will effective and efficient to apply on this dimension and will reduce the complexity of the database size.
- Implementation: We'll add "created_date", "update_date" to the dim_payments

**Geolocation Dimension**
SCD Type: Type 2
- Reason: Although not explicitly mentioned by the client, geolocation data can change over time, and historical accuracy might be important for geographical analysis.
- Implementation: We'll add "created_date", "expired_date", and "current_flag" columns to the dim_geolocation table.

**Review Dimension**
- SCD Type: Type 1
- Reason: Reviews are typically not changed after submission, but if they are, we usually want to see the most recent version.
- Implementation: We'll update the existing row if any changes occur.

**Date Dimension**
- SCD Type: 0
- Reason: Date dimensions are typically static and don't change over time.


These SCD strategies will allow us to:

Track full history for products, supporting product evolution analysis.
Maintain current and previous states for customer and seller addresses.
Keep the most up-to-date information for attributes that don't require historical tracking.
Align with the client's data retention policies.


# Slowly Changing Dimension (SCD) Implementation

| Dimension   | SCD Type | Retention Policy | Rationale |
|-------------|----------|------------------|-----------|
| Product     | Type 2   | Indefinite       | Full history needed for product evolution analysis. Price and category changes are particularly important. |
| Customer    | Type 2   | 3 years | Address changes need historical tracking. Other attributes only need current state. |
| Seller      | Type 2   | 3 years | Address changes need historical tracking. Other attributes only need current state. |
| Geolocation | Type 2   | 3 years          | Historical accuracy important for geographical analysis. |
| Review      | Type 1   | N/A              | Most recent version of reviews is typically sufficient. |
|Payment      | Type 1   | N/A              | Most recent version of payments is typically sufficient.
| Date        | None     | N/A              | Date dimension is static and doesn't change. |

## Implementation Details

- For Type 2 SCD:
  - Add columns: `created_date`, `updated_date`, `current_flag`
  - Insert new row for changes, update dates and flags accordingly

- For Type 1 SCD:
  - Simply update the existing row when changes occur
  - Add columns: `created_date`, `updated_date`


This SCD implementation strategy balances the need for historical tracking with performance and storage considerations, providing a robust solution for the Olist data warehouse.

# ELT WORKFLOW
In this project, we will use 2 separate database. The first database is the original data source, and the second database act as datawarehouse.
In first database, consist only 1 schema, and for second database consist of 3 schema: sources, staging, and final. the data from original database will be extracted and loaded first into sources schema. the data from sources schema, will be loaded into staging schema.

![FIGURE](https://github.com/user-attachments/assets/8f39000d-75ef-4db5-b101-0192e8566a5a)

# ELT PIPELINE ORCHESTRATION
ELT Pipeline Orchestration refers to the process of managing, scheduling, and automating the various tasks and workflows involved in an Extract, Load, and Transform (ELT) data pipeline. In an ELT architecture, data is first extracted from various sources, loaded into a data warehouse or data lake, and then transformed within the storage layer.

![FIGURE](https://github.com/user-attachments/assets/4896131f-d994-4504-a385-09afc3038c2c)

Tools:
- Orchestration: Luigi
- Schedulling: Cron
- Write summary : pandas
- Logging: logging.info for write logs for every step of the proccess and logging.error for write error
- alerting & notification: sentry. you can visit the sentry website to read the documentation in this [link](https://sentry.io/welcome/)

 How to use this repo:
## 1. Requirements
- **OS:**
  - Linux
  - WSL
- **Tools:**
  - Dbeaver
  - Docker
  - Cron
- **Programming Language:**
  - Python
  - SQL
- **Python Library:**
  - Luigi
  - Pandas
  - Sentry-SDK
- **Platforms**
   - Sentry

## 2.Preparations
- Clone this repo using the following command:

  ```bash
  git lfs clone https://github.com/ibnufajar1994/elt-data-warehouse.git
  ```
run the command below on the terminal:
  ```bash
  docker compose up -d
  ```
  
- Create Sentry Project
  - visit: https://www.sentry.io
  - Signup with email that you want get notifications and alert
  - Create Project on sentry:
    - use python as a platform
    - set alert frequency as "on every new issue"
    - Create project name
    - **Copy the DSN of your project into .env file**

- Create temp dir. on your root project directory:
```bash
mkdir pipeline/temp/data
mkdir pipeline/temp/log
```
- Create & use virtual environment on your root directory project
- Install the requirements using the following command:
```bash
pip install -r requirements.txt
```

- **Create env file in your root project directory, copy this variable into it.you need to adjust the value based on your preferences:**
  
```bash
SRC_POSTGRES_DB=...
SRC_POSTGRES_HOST=...
SRC_POSTGRES_USER=...
SRC_POSTGRES_PASSWORD=...
SRC_POSTGRES_PORT=...

# DWH
DWH_POSTGRES_DB=...
DWH_POSTGRES_HOST=...
DWH_POSTGRES_USER=...
DWH_POSTGRES_PASSWORD=...
DWH_POSTGRES_PORT=...

# SENTRY DSN
SENTRY_DSN=... # Fill with your Sentry DSN Project 

# DIRECTORY
# Adjust with your directory. make sure to write full path
DIR_ROOT_PROJECT=...     # <project_dir>
DIR_TEMP_LOG=...         # <project_dir>/pipeline/temp/log
DIR_TEMP_DATA=...        # <project_dir>/pipeline/temp/data
DIR_EXTRACT_QUERY=...    # <project_dir>/pipeline/src_query/extract
DIR_LOAD_QUERY=...       # <project_dir>/pipeline/src_query/load
DIR_TRANSFORM_QUERY=...  # <project_dir>/pipeline/src_query/transform
DIR_LOG=...              # <project_dir>/logs/
```
- run this command on the backround process:
  ```bash
  luigid --port 8082 &
  ```
- you can run this command directly on the terminal to run the pipeline:
   ```bash
   python3 elt_main.py
   ```
- or you can schedulling using cron, for example on the code below is the command to run the pipeline every one hour.
    ```bash
   0 * * * * <project_dir>/elt_run.sh
   ```


