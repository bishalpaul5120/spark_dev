from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
import pendulum

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
    description="Orchestrate Spark jobs on EMR using Airflow",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=default_args,
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
            "ActionOnFailure": "TERMINATE_CLUSTER",
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
            "ActionOnFailure": "TERMINATE_CLUSTER",
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
            "ActionOnFailure": "TERMINATE_CLUSTER",
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
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    watch_last_step = EmrStepSensor(
        task_id="watch_last_spark_step",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ ti.xcom_pull(task_ids='add_spark_steps')[-1] }}",
        aws_conn_id="aws_default",
    )

    terminate_emr = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )

    # -------------------------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------------------------
    create_emr_cluster >> add_spark_steps >> watch_last_step >> terminate_emr