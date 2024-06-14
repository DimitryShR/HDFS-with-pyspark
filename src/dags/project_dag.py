import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

GEO_EVENTS_PATH = "/user/master/data/geo/events"
GEO_CITY_PATH = "/user/dimitrym/data/geo/geo"
PROJECT_S7_PATH = "/user/dimitrym/project_s7"

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
dag_id = "project_s7",
default_args=default_args,
schedule_interval=None,
)

project_step_1 = SparkSubmitOperator(
task_id='project_step_1',
dag=dag_spark,
application ='/lessons/project_step_1.py' ,
conn_id= 'yarn_spark',
application_args = [f"{GEO_EVENTS_PATH}", f"{GEO_CITY_PATH}", f"{PROJECT_S7_PATH}/project_step_1"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 2,
executor_memory = '3g'
)

project_step_2 = SparkSubmitOperator(
task_id='project_step_2',
dag=dag_spark,
application ='/lessons/project_step_2.py' ,
conn_id= 'yarn_spark',
application_args = [f"{GEO_EVENTS_PATH}", f"{GEO_CITY_PATH}", f"{PROJECT_S7_PATH}/project_step_2"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 2,
executor_memory = '3g'
)

# Установил минимальное расстояние 100 км, т.к. иначе нужно искать дату, где события не общающихся подписчиков находились на расстоянии менее 1 км
project_step_3 = SparkSubmitOperator(
task_id='project_step_3',
dag=dag_spark,
application ='/lessons/project_step_3.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-06-15", "100", f"{GEO_EVENTS_PATH}", f"{GEO_CITY_PATH}", f"{PROJECT_S7_PATH}/project_step_3"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 2,
executor_memory = '3g'
)

[project_step_1 >> project_step_2 >> project_step_3]