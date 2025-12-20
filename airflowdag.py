from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.utils.state import State
from datetime import timedelta

# -------------------
# GLOBAL CONFIG
# -------------------
AWS_CONN_ID = "aws_default"
REGION = "ap-south-1"

SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:123456789012:airflow-emr-alerts"

SPARK_DEPLOYMENT_BUCKET = "s3://master-job/spark_deployment"
BOOTSTRAP_BUCKET = "s3://snowflake-creds/snowbootstrap.sh"

# -------------------
# FAILURE CALLBACK
# -------------------
def notify_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exec_date = context['execution_date']
    log_url = context['task_instance'].log_url

    message = f"""
    ❌ Airflow Task Failed

    DAG: {dag_id}
    Task: {task_id}
    Execution Time: {exec_date}

    Logs: {log_url}
    """

    sns_hook = SnsHook(aws_conn_id=AWS_CONN_ID)
    sns_hook.publish_to_topic(
        topic_arn=SNS_TOPIC_ARN,
        message=message,
        subject="Airflow EMR Pipeline Failure"
    )

# -------------------
# DEFAULT ARGS
# -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

# -------------------
# EMR CLUSTER CONFIG
# -------------------
JOB_FLOW_OVERRIDES = {
    "Name": "spark-airflow-cluster",
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
                "Path": BOOTSTRAP_BUCKET
            },
        }
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
    "LogUri": "s3://master-job/emr-logs/",
}

# -------------------
# SPARK STEPS
# -------------------
SPARK_STEPS = [
    {
        "Name": "S3 Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
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
                "--deploy-mode", "cluster",
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
                "--deploy-mode", "cluster",
                f"{SPARK_DEPLOYMENT_BUCKET}/master.py",
            ],
        },
    },
]

# -------------------
# DAG
# -------------------
with DAG(
    dag_id="emr_spark_s3_snowflake_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
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
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id=AWS_CONN_ID,
    )

    watch_master_step = EmrStepSensor(
        task_id="watch_master_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_spark_steps', key='return_value')[-1] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",  # 🔥 ensures termination even on failure
    )

    create_emr_cluster >> add_spark_steps >> watch_master_step >> terminate_emr_cluster
