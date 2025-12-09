from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


# S3 bucket where raw daily CSVs land
LANDING_BUCKET = "landing-zone-datalake"

# The name of the Lambda function in AWS (deployed from the Docker image)
LAMBDA_FUNCTION_NAME = "card-transaction-lambda"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="card_transaction_etl_dag",
    default_args=default_args,
    start_date=datetime(2025, 11, 10),
    schedule_interval="@daily",  # we expect one file per day
    catchup=False,
    max_active_runs=1,
    tags=["etl", "lambda", "s3"],
) as dag:
    """
    This DAG runs once per day.

    For each execution date (ds = YYYY-MM-DD):
    - it builds the expected S3 key for that day's file
    - waits until that file appears in S3
    - then invokes the Lambda that does the CSV -> JSONL processing
    """

    # Build the expected S3 key for today's file,
    # following the folder layout from the task.
    s3_key = (
        "datalake/prod/datasource=partnera/client=clienta/entity=card_transaction/"
        "year={{ ds[:4] }}/"
        "month={{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}/"
        "day={{ macros.ds_format(ds, '%Y-%m-%d', '%d') }}/"
        "card_transaction_{{ ds }}.csv"
    )

    # 1) Wait for today's file to land in S3
    wait_for_file = S3KeySensor(
        task_id="wait_for_card_transaction_file",
        bucket_name=LANDING_BUCKET,
        bucket_key=s3_key,
        wildcard_match=False,
        poke_interval=60,         # check every 60 seconds
        timeout=60 * 60 * 6,      # give the file up to 6 hours to arrive
        mode="reschedule",        # free up worker slots between checks
    )

    # 2) Invoke the Lambda once the file is available
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_card_transaction_lambda",
        function_name=LAMBDA_FUNCTION_NAME,
        payload="""
        {
            "bucket": "{{ params.bucket }}",
            "key": "{{ params.key }}"
        }
        """,
        params={
            "bucket": LANDING_BUCKET,
            "key": s3_key,
        },
        log_type="Tail",          # stream CloudWatch logs into Airflow logs
        invocation_type="RequestResponse",
    )

    # The Lambda should only run after the file is confirmed to exist
    wait_for_file >> invoke_lambda
