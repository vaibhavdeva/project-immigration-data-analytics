import lib.emr_lib as emr
import os
import logging
 
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1, 0, 0, 0 ,0),
    'end_date' : datetime(2016, 12, 1, 0, 0, 0 ,0),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True,
}

# Initialize the DAG
dag = DAG('immigration_analytics_pipeline', concurrency=1, schedule_interval="@monthly", default_args=default_args, max_active_runs= 1)
region = emr.get_region()
emr.client(region_name=region)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='cluster', num_core_nodes=2)
    Variable.set("cluster_id", cluster_id)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)
    #
    Variable.set("cluster_id", "na")
    Variable.set("dag_analytics_state", "na")
    Variable.set("dag_normalize_state", "na")

def submit_pyspark_to_emr():

    # create a spark session and wait for it to into idle mode
    cluster_id = Variable.get("cluster_id")
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)

    # Get execution date format
    execution_date = kwargs["execution_date"]
    month = execution_date.strftime("%b").lower()
    year = execution_date.strftime("%y")
    logging.info(month+year)

    # submit pyspark code to the spark session created
    statement_response = emr.submit_statement(session_url, 
                                kwargs['params']['file'], 
                                "month_year = '{}'\n".format(month+year))
    logs = emr.track_statement_progress(cluster_dns, statement_response.headers)

    # kill teh spark session once the job is done
    emr.kill_spark_session(session_url)

    # check if job failed or completed
    if kwargs['params']['log']:
        for line in logs:
            if 'FAIL' in line:
                logging.info(line)
                raise AirflowException("Analytics data Quality check Fail!")

# dummy start and end operators
begin = DummyOperator(
    task_id='begin',
    dag=dag
    )

end = DummyOperator(
    task_id='end',
    dag=dag
    )
# cluster tasks
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# build tasks
build_reference = PythonOperator(
    task_id='build_reference',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__reference.py', "log" : False},
    dag=dag
)

build_airport = PythonOperator(
    task_id='build_airport',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__airport.py', "log" : False},
    dag=dag
)

build_demographics = PythonOperator(
    task_id='build_demographics',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__demographics.py', "log" : False},
    dag=dag
)

build_weather = PythonOperator(
    task_id='build_weather',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__weather.py', "log" : False},
    dag=dag
)

build_immigration = PythonOperator(
    task_id='build_immigration',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__immigration.py', "log" : False},
    dag=dag
)

# analytics tasks
build_analytics_airport = PythonOperator(
    task_id='build_analytics_airport',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__analytics_airport.py', "log" : True},
    dag=dag
)

build_analytics_immigration = PythonOperator(
    task_id='build_analytics_immigration',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/build__analytics_immigration.py', "log" : True},
    dag=dag
)

# data quality check
check_builds = PythonOperator(
    task_id='check_builds',
    python_callable = submit_pyspark_to_emr,
    params={"file" : '/root/airflow/dags/transform/check__builds.py', "log" : False},
    dag=dag
)


# create DAG
begin >> create_cluster

create_cluster >> wait_for_cluster_completion

wait_for_cluster_completion >> build_reference

build_reference >> [build_airport, build_demographics, build_weather, build_immigration]

[build_airport, build_demographics, build_weather, build_immigration] >> [build_analytics_airport, build_analytics_immigration]

[build_analytics_airport, build_analytics_immigration] >> check_builds

check_builds >> terminate_cluster

terminate_cluster >> end