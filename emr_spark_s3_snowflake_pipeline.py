from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime

JOB_FLOW_OVERRIDES = {
    "Name": "airflow-spark-emr-cluster",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}],

    # 🔑 CRITICAL ROLES (you confirmed these exist)
    "ServiceRole": "EMR_DefaultRole",
    "JobFlowRole": "EMR_EC2_DefaultRole",

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

        # 🔥 MUST be ONE PUBLIC SUBNET
        "Ec2SubnetId": "subnet-05ba720e50f4b37ba",

        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },

    "VisibleToAllUsers": True,
    "LogUri": "s3://master-job/emr-logs/",
}

SPARK_STEPS = [
    {
        "Name": "S3 Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://master-job/spark_deployment/s3job.py",
            ],
        },
    },
    {
        "Name": "Snowflake Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://master-job/spark_deployment/snowjob.py",
            ],
        },
    },
    {
        "Name": "Master Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://master-job/spark_deployment/master.py",
            ],
        },
    },
]

with DAG(
    dag_id="emr_spark_s3_snowflake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["emr", "spark", "snowflake"],
) as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=SPARK_STEPS,
    )

    watch_steps = EmrStepSensor(
        task_id="watch_master_step",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ ti.xcom_pull(task_ids='add_spark_steps')[2] }}",
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        trigger_rule="all_done",
    )

    create_cluster >> add_steps >> watch_steps >> terminate_cluster
