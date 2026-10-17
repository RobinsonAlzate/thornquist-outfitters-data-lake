# Thornquist Outfitters Data Lake & Analytics Pipeline

An automated cloud data pipeline built on AWS utilizing **Amazon S3**, **AWS Glue (PySpark)**, and **Amazon Athena** to ingest, transform, and serve conformed retail and e-commerce analytics for Thornquist Outfitters.

---

## 🏗️ Project Architecture & Workflow

1. **Raw Data Ingestion:** Uploads POS sales, e-commerce orders, and legacy customer files to structured raw prefixes in S3.
2. **AWS Glue ETL Processing:** Cleans, maps, flattens, and unifies multi-source datasets using PySpark and writes them into partitioned Parquet/JSON formats in the **Trusted** and **Curated** layers.
3. **Analytics & Views:** Automatically provisions external tables, repairs partitions, and creates analytical SQL views in Amazon Athena.

---

## 📁 Repository Structure

```text
thornquist-outfitters-data-lake/

├── cloudformation/
│   └── cloudformation_thornquist_infrastructure1.yaml
├── data/
│   ├── thornquist_customers.txt
│   ├── thornquist_ecommerce_orders.json
│   └── thornquist_pos_sales.csv
├── report/
│   └── S3 Datalake Report.docx
├── scripts/
│   └── glue_etl_pipeline.py
│└── sql/
│   ├── 01_raw_pos_data_preview.sql
│   ├── 02_create_curated_sales_table.sql
│   ├── 03_msck_repair_curated_sales.sql
│   ├── 04_preview_curated_sales.sql
│   ├── 05_create_sales_by_category_view.sql
│   └── 06_query_sales_by_category.sql
├── .gitignore
├── deploy.py
├── README.md
```

---

## 🚀 Step-by-Step Deployment Guide

Follow these line-by-line instructions to deploy the pipeline successfully.

### Step 1: Clone the Repository & Set Up Your Environment

Open your terminal (PowerShell, Git Bash, or terminal of your choice), clone the repository directly into a new folder on your Desktop, and enter the project directory:

**For Windows (PowerShell):**
```powershell
Set-Location $HOME\Desktop; New-Item -ItemType Directory -Force -Path "thornquist-outfitters-data-lake"; Set-Location "thornquist-outfitters-data-lake"; git clone https://github.com/YOUR_USERNAME/thornquist-outfitters-data-lake.git .
```

**For Mac / Linux / Git Bash:**
```bash
cd ~/Desktop && mkdir -p thornquist-outfitters-data-lake && cd thornquist-outfitters-data-lake && git clone https://github.com/YOUR_USERNAME/thornquist-outfitters-data-lake.git .
```

Ensure you have **Python 3.x** installed, then install the required AWS SDK (`boto3`):
```bash
pip install boto3
```

---

### Step 2: Configure AWS Academy Session Credentials

Because this project runs inside an AWS Academy Learner Lab, you must use your temporary session tokens:

1. Log into your AWS Academy Learner Lab and click **AWS Details** -> **AWS CLI**.
2. Copy your temporary credentials (`aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token`).
3. Create and open your credentials file automatically using one of the commands below:

   * **Windows (PowerShell):**
     ```powershell
     New-Item -ItemType Directory -Force -Path "$HOME\.aws"; New-Item -ItemType File -Force -Path "$HOME\.aws\credentials"; notepad "$HOME\.aws\credentials"
     ```

   * **Mac / Linux:**
     ```bash
     mkdir -p ~/.aws && touch ~/.aws/credentials && open ~/.aws/credentials || nano ~/.aws/credentials
     ```

4. Paste your active session tokens into the opened file in this exact format:
   ```ini
   [default]
   aws_access_key_id = ASIA...
   aws_secret_access_key = ...
   aws_session_token = IQoJb3JpZ2luX2Vj...
   ```
   *(Note: Remember that AWS Academy session tokens expire frequently; if you get token errors, simply regenerate and update this file).*

---

### Step 3: Create the Glue Job Manually (One-Time Setup)

Due to cross-account IAM restrictions (`LabRole`) in the AWS Academy environment, the Glue ETL job shell must be initialized manually in the AWS Console once before running the deployment script.

1. Open the **AWS Management Console** and navigate to **AWS Glue**.
2. In the left sidebar, click on **ETL jobs** under *ETL jobs*.
3. Click **Create job**.
4. Configure the job with these exact settings:
   * **Name**: `thornquist-etl-pipeline`
   * **IAM Role**: Select `LabRole`
   * **Type**: Spark (Python 3)
   * **Glue version**: Spark 3.x / 5.x (compatible with your setup)
   * **Script file name**: `glue_etl_pipeline.py`
   * **Script path**: `s3://thornquist-outfitters-am-rob-alx/scripts/`
   * **Temporary path**: `s3://thornquist-outfitters-am-rob-alx/athena-results/temp/`
5. Click **Save** in the top-right corner (do not click Run yet).

---

### Step 4: Run the Automated Deployment Script

With your credentials configured and the Glue job template saved in AWS, return to your local terminal inside the project directory and run:

```bash
python deploy.py
```

The script will automatically execute the complete pipeline sequentially:
* **[1/5]** Uploads raw source files and the refactored PySpark script to your S3 bucket.
* **[2/5]** Verifies and ensures the Glue Database (`thornquist_db`) exists.
* **[3/5]** Triggers the `thornquist-etl-pipeline` Glue Job and monitors its live execution status until it reports **`SUCCEEDED`**.
* **[4/5]** Executes Athena DDL queries, repairs table partitions, and sets up analytical views.
* **[5/5]** Confirms successful end-to-end deployment.
