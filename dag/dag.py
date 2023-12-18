from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
import configparser



### Variables 
config = configparser.ConfigParser()
config.read('airflow/dags/private.cfg')

S3_DATA_SOURCE = config.get('AWS', 'S3_DATA_SOURCE')
S3_OUTPUT = config.get('AWS', 'S3_OUTPUT')
BUCKET_NAME = config.get('AWS', 'BUCKET_NAME')
s3_menu_data = config.get('AWS', 's3_menu_data')
s3_restau_data = config.get('AWS', 's3_restau_data')
S3_SCRIPT = config.get("AWS", 'S3_SCRIPT')
S3_SCRIPT_PY = config.get('AWS', 's3_script_py')
MENU_DATA_LOCAL = 'airflow/datasets/restaurant-menus.csv'
RESTAU_DATA_LOCAL = 'airflow/datasets/restaurants.csv'
SCRIPT_LOCAL = 'airflow/dags/main.py'
KEY_PAIR = config.get('AWS', 'KEY_PAIR')
EMR_LOG_URI = config.get('AWS', 'EMR_LOG_URI')
GLUE_ROLE = config.get('AWS', 'GLUE_ROLE')
GLUE_DB = config.get('AWS', 'GLUE_DB')


### Load file to S3
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)



### Crawler configuration 
def glue_config(s3_key_table, glue_role, glue_db, s3_location_tables):
    glue_crawler_config = {
    "Name": s3_key_table,
    "Role": glue_role,
    "DatabaseName": glue_db,
    "Targets": {"S3Targets": [{"Path": f"{s3_location_tables}{s3_key_table}.avro"}]}
}
    return glue_crawler_config



### EMR cluster configuration
emr_cluster_configuration = {
    "Name": "uber-eats-cluster",
    "ReleaseLabel": "emr-6.12.0",
    "LogUri": EMR_LOG_URI,
    "Applications" : [{"Name": "Spark"}],
    "Instances": {
        "Ec2SubnetId" : "subnet-0b28afc731fb2ab62",
        "InstanceGroups": [
            {
                "Name": "Master node",
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
                "InstanceCount": 3,
            },
        ],
        "Ec2KeyName": KEY_PAIR,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


### Spark step 
spark_step = [{
    "Name": "MyPythonSparkStep",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
             S3_SCRIPT,
            "--data_source", S3_DATA_SOURCE,
            "--output", S3_OUTPUT,
        ],
    },
}
]


### Dag arguments
default_args = {
    'owner': 'dorianteffo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}



with DAG(
    'ubereats_emr_spark', 
    description = 'load data to s3, and run a spark job on EMR',
    tags=['AWS S3', 'SPARK', 'EMR'],
    start_date = datetime(2023, 11, 13),
    default_args = default_args, 
) as dag : 


    ### Restaurants menus data  to S3 
    menu_data_to_s3 = PythonOperator(
        task_id="menu_data_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": MENU_DATA_LOCAL, "key": s3_menu_data}
    )


    ### Restaurants data to S3
    restau_data_to_s3 = PythonOperator(
        task_id="restaurant_data_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": RESTAU_DATA_LOCAL, "key": s3_restau_data}
    )


    ### Python script (spark) to S3
    script_to_s3 = PythonOperator(
        task_id="script_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": SCRIPT_LOCAL, "key": S3_SCRIPT_PY},
    )


    ### EMR cluster creation
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=emr_cluster_configuration,
        aws_conn_id="amazon_connection_id",
        emr_conn_id="emr_connection_id",
        region_name ="eu-west-3"
    )


    ### Add step to the emr cluster 
    step_adder = EmrAddStepsOperator(
        task_id="add_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="amazon_connection_id",
        steps=spark_step,
        params={ 
            "S3_DATA_SOURCE": S3_DATA_SOURCE,
            "S3_SCRIPT": S3_SCRIPT,
            "S3_OUTPUT": S3_OUTPUT,
        },
    )


    ### Wait for the step to complete
    wait_for_step = EmrStepSensor(
    task_id="wait_for_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
    aws_conn_id="amazon_connection_id",
    )


    ### Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="amazon_connection_id",
    )


    ### Restaurant table crawler
    crawl_restaurant = GlueCrawlerOperator(
        task_id="crawl_restaurant",
        config=glue_config('restaurant_table',GLUE_ROLE,GLUE_DB,S3_OUTPUT),
    )


    ### Menu table crawler 
    crawl_menu = GlueCrawlerOperator(
    task_id="crawl_menu",
    config=glue_config('menu_table',GLUE_ROLE,GLUE_DB,S3_OUTPUT),
    )


    ### Address table crawler 
    crawl_address = GlueCrawlerOperator(
    task_id="crawl_address",
    config=glue_config('address_table',GLUE_ROLE,GLUE_DB,S3_OUTPUT),
    )

    menu_data_to_s3 >> script_to_s3
    restau_data_to_s3 >> script_to_s3
    script_to_s3 >> create_emr_cluster >> step_adder >> wait_for_step >>  terminate_emr_cluster
    terminate_emr_cluster >> crawl_restaurant
    terminate_emr_cluster >> crawl_menu
    terminate_emr_cluster >> crawl_address
  

