import boto3
import time
import sys

# Configuration
BUCKET_NAME = "thornquist-outfitters-am-rob-alx"
REGION = "us-east-1"
DATABASE = "thornquist_db"
GLUE_JOB_NAME = "thornquist-etl-pipeline"

s3 = boto3.client('s3', region_name=REGION)
glue = boto3.client('glue', region_name=REGION)
athena = boto3.client('athena', region_name=REGION)

def upload_assets():
    print("🚀 [1/3] Uploading raw data and scripts to S3...")
    
    # Upload Data Files
    s3.upload_file("data/thornquist_pos_sales.csv", BUCKET_NAME, "raw/pos/thornquist_pos_sales.csv")
    s3.upload_file("data/thornquist_ecommerce_orders.json", BUCKET_NAME, "raw/ecommerce/thornquist_ecommerce_orders.json")
    s3.upload_file("data/thornquist_customers.txt", BUCKET_NAME, "raw/legacy-customers/thornquist_customers.txt")
    
    # Upload Glue Script
    s3.upload_file("scripts/glue_etl_pipeline.py", BUCKET_NAME, "scripts/glue_etl_pipeline.py")
    print("✅ Files successfully uploaded to S3!")

def run_glue_job():
    print(f"⚙️ [2/3] Starting Glue ETL Job: {GLUE_JOB_NAME}...")
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)
    job_run_id = response['JobRunId']
    
    while True:
        status_resp = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        status = status_resp['JobRun']['JobRunState']
        print(f"   Status: {status}...")
        
        if status == 'SUCCEEDED':
            print("✅ Glue ETL Job completed successfully!")
            break
        elif status in ['FAILED', 'STOPPED']:
            print(f"❌ Glue ETL Job failed with status: {status}")
            sys.exit(1)
        
        time.sleep(15)

def run_athena_queries():
    print("📊 [3/3] Executing Athena Partition Repair and Analytical Views...")
    
    queries = [
        "MSCK REPAIR TABLE thornquist_db.curated_sales;",
        """
        CREATE OR REPLACE VIEW thornquist_db.v_sales_by_category AS
        SELECT 
            category,
            COUNT(order_id) AS total_orders,
            SUM(quantity) AS total_items_sold,
            ROUND(SUM(unit_price * quantity), 2) AS total_revenue
        FROM thornquist_db.curated_sales
        GROUP BY category;
        """
    ]
    
    for q in queries:
        response = athena.start_query_execution(
            QueryString=q,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': f"s3://{BUCKET_NAME}/athena-results/"}
        )
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to finish
        while True:
            q_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = q_status['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                raise Exception(f"Athena query failed: {q_status['QueryExecution']['Status'].get('StateChangeReason')}")
            time.sleep(3)
            
    print("✅ Athena tables repaired and analytical views created successfully!")

if __name__ == "__main__":
    upload_assets()
    run_glue_job()
    run_athena_queries()
    print("🎉 All pipeline steps executed automatically from start to finish!")