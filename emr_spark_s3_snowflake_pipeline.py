from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.sns import SnsHook

# -------------------------------------------------------------------
# GLOBAL CONFIG
# -------------------------------------------------------------------
AWS_CONN_ID = "aws_default"
REGION = "ap-south-1"

SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:123456789012:airflow-emr-alerts"

SPARK_DEPLOYMENT_BUCKET = "s3://master-job/spark_deployment"
BOOTSTRAP_SCRIPT = "s3://snowflake-creds/snowbootstrap.sh"

# -------------------------------------------------------------------
# FAILURE NOTIFICATION CALLBACK
# -------------------------------------------------------------------
def notify_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    message = f"""
❌ Airflow Task Failed

DAG: {dag_id}
Task: {task_id}
Execution Time: {execution_date}

Logs:
{log_url}
"""

    sns_hook = SnsHook(aws_conn_id=AWS_CONN_ID)
    sns_hook.publish_to_topic(
        topic_arn=SNS_TOPIC_ARN,
        message=message,
        subject="Airflow EMR Pipeline Failure",
    )

# -------------------------------------------------------------------
# DEFAULT ARGS
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

# -------------------------------------------------------------------
# EMR CLUSTER CONFIG
# -------------------------------------------------------------------
JOB_FLOW_OVERRIDES = {
    "Name": "airflow-spark-emr-cluster",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "BootstrapActions": [
        {
            "Name": "Snowflake Spark Connector",
            "ScriptBootstrapAction": {
                "Path": BOOTSTRAP_SCRIPT
            },
        }
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
    "LogUri": "s3://master-job/emr-logs/",
}

# -------------------------------------------------------------------
# SPARK STEPS
# -------------------------------------------------------------------
SPARK_STEPS = [
    {
        "Name": "S3 Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                f"{SPARK_DEPLOYMENT_BUCKET}/s3job.py",
            ],
        },
    },
    {
        "Name": "Snowflake Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                f"{SPARK_DEPLOYMENT_BUCKET}/snowjob.py",
            ],
        },
    },
    {
        "Name": "Master Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                f"{SPARK_DEPLOYMENT_BUCKET}/master.py",
            ],
        },
    },
]

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id="emr_spark_s3_snowflake_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["emr", "spark", "snowflake"],
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
    )

    add_spark_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=SPARK_STEPS,
        aws_conn_id=AWS_CONN_ID,
    )

    watch_master_step = EmrStepSensor(
        task_id="watch_master_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[-1] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )

    create_emr_cluster >> add_spark_steps >> watch_master_step >> terminate_emr_cluster
