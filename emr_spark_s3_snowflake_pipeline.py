from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.models import Variable

# -------------------------------------------------------------------
# DEFAULT ARGS
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}

# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="emr_spark_s3_snowflake_pipeline",
    default_args=default_args,
    description="Orchestrate Spark jobs on EMR using Airflow",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["emr", "spark", "snowflake"],
) as dag:

    # -------------------------------------------------------------------
    # EMR CLUSTER CONFIG
    # -------------------------------------------------------------------
    JOB_FLOW_OVERRIDES = {
        "Name": "airflow-emr-spark-cluster",
        "ReleaseLabel": "emr-6.10.0",
        "Applications": [{"Name": "Spark"}],
        "LogUri": "s3://aws-logs-093711202752-ap-south-1/emr/",
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
        "VisibleToAllUsers": True,
    }

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    # -------------------------------------------------------------------
    # SPARK STEPS
    # -------------------------------------------------------------------
    SPARK_STEPS = [
        {
            "Name": "s3-job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "s3://master-job/spark_deployment/s3job.py",
                ],
            },
        },
        {
            "Name": "snowflake-job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--jars",
                    "s3://snowflake-creds/jars/spark-snowflake_2.12-3.1.1.jar,"
                    "s3://snowflake-creds/jars/snowflake-jdbc-3.18.1.jar",
                    "s3://master-job/spark_deployment/snowjob.py",
                ],
            },
        },
        {
            "Name": "master-job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "s3://master-job/spark_deployment/master.py",
                ],
            },
        },
    ]

    add_spark_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    watch_steps = EmrStepSensor(
        task_id="watch_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[2] }}",
        aws_conn_id="aws_default",
    )

    terminate_emr = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )

    # -------------------------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------------------------
    create_emr_cluster >> add_spark_steps >> watch_steps >> terminate_emr
