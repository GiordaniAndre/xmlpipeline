from airflow import DAG
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'neo4j_data_pipeline',
    default_args=default_args,
    description='Process XML data and ingest into Neo4j',
    schedule_interval=None,
    start_date=datetime(2023, 3, 22),
    catchup=False,
)

landing_zone = HdfsSensor(
    task_id='landing_zone',
    filepath='/data/raw/tripdata',
    hdfs_conn_id='hdfs_default',
    dag=dag)

spark_master = "local"
spark_app_name = "neo4j_data_pipeline"

create_constraints_query = """
CREATE CONSTRAINT ON (p:Protein) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (g:Gene) ASSERT g.id IS UNIQUE;
CREATE CONSTRAINT ON (f:Feature) ASSERT f.id IS UNIQUE;
CREATE CONSTRAINT ON (r:Reference) ASSERT r.id IS UNIQUE;
CREATE CONSTRAINT ON (a:Author) ASSERT a.id IS UNIQUE;
CREATE CONSTRAINT ON (fn:FullName) ASSERT fn.id IS UNIQUE;
CREATE CONSTRAINT ON (o:Organism) ASSERT o.id IS UNIQUE;
"""

create_data_model_task = Neo4jOperator(
    task_id='create_data_model',
    query=create_constraints_query,
    neo4j_conn_id='neo4j_default',
    dag=dag,
)


process_to_neo4j = SparkSubmitOperator(
    task_id="process_to_neo4j",
    application="/usr/local/spark/app/process_to_neo4j.py",
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[],
    jars="/opt/spark-xml_2.12-0.14.0.jar", # Add this line
    executor_memory="2G",
    executor_cores=1,
    num_executors=1,
    dag=dag)



# Set up task dependencies
landing_zone >> create_data_model_task >> process_to_neo4j
