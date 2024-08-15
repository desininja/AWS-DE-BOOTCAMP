from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrStepOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# DAG Configuration
S3_BUCKET = 'your-bucket-name'
S3_KEY_PATTERN = 'your-path-to-file/*.csv'  # Pattern to match the file
EMR_CLUSTER_ID = 'j-XXXXXXXXXXXX'  # Replace with your EMR cluster ID
SPARK_SCRIPT_PATH = 's3://your-script-bucket/spark-script.py'  # Path to the Spark script in S3
SPARK_OUTPUT_PATH = 's3://your-output-bucket/output/'  # Output path for processed data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_s3_key(**context):
    # Get the detected S3 key from XComs
    s3_key = context['task_instance'].xcom_pull(task_ids='check_s3_for_file')
    # Print or log the S3 key for debugging
    print(f"Processing file: {s3_key}")
    # Return the key for use in subsequent tasks
    return s3_key

# Define the DAG
dag = DAG(
    's3_to_emr_spark_with_xcoms',
    default_args=default_args,
    description='DAG to trigger EMR Spark job based on S3 file arrival using XComs',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Define tasks

# Sensor Task to check the arrival of the file in S3
check_s3_for_file = S3KeySensor(
    task_id='check_s3_for_file',
    bucket_name=S3_BUCKET,
    bucket_key=S3_KEY_PATTERN,
    wildcard_match=True,
    aws_conn_id='aws_default',
    timeout=18*60*60,  # 18 hours
    poke_interval=60,  # check every 60 seconds
    dag=dag,
)

# Python Operator to process the S3 key and use XComs
process_s3 = PythonOperator(
    task_id='process_s3_key',
    python_callable=process_s3_key,
    provide_context=True,
    dag=dag,
)

# Spark Job Step on EMR, using the S3 key passed via XComs
emr_spark_step = EmrStepOperator(
    task_id='emr_spark_step',
    job_flow_id=EMR_CLUSTER_ID,
    aws_conn_id='aws_default',
    step_name='Process S3 File with Spark',
    step_type='spark',
    step_args=[
        'spark-submit', '--deploy-mode', 'cluster',
        SPARK_SCRIPT_PATH, "{{ task_instance.xcom_pull(task_ids='process_s3_key') }}", SPARK_OUTPUT_PATH
    ],
    dag=dag,
)

# Set task dependencies
check_s3_for_file >> process_s3 >> emr_spark_step
