import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator


job_name = 'carte_carto-1.0'
jar_file = 'hdfs:/user/allieart/cartographie/carte_carto-1.0.jar' # 'hdfs:/projects/k******/carte_cartographique/carte_carto-1.0.jar'
conf_path = '/user/allieart/cartographie/conf' # '/projects/k******/carte_cartographique/conf' 
output_path = '/user/allieart/cartographie/output' # '/projects/k******/carte_cartographique/output' 
hdfs_uri = 'hdfs://endor.l*****.local:8020'
conn_id = 'arthur_con_kashyyyk'


DEFAULT_ARGS = {
    'owner': 'allieart',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 22),
    'email': ['arthur.allie@g******.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG(
    'data_mapping_job',
    catchup=False,
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=1),
	schedule_interval = '0 1 * * *'
)


# def create_spark_submit_cmd(job_name, jar_file, conf_path, output_path, hdfs_uri, 
#                                 num_executors = 3, executor_cores = 5, executor_memory = '12g', driver_memory = '8g'):
#     return f"""export SPARK_MAJOR_VERSION=2 
#     spark-submit \
#     --name {job_name} \
#     --master yarn \
#     --num-executors {num_executors}  \
#     --executor-cores {executor_cores} \
#     --executor-memory {executor_memory} \
#     --driver-memory {driver_memory} \
#     --class com.l******.App \
#     --jars {jar_file} \
#     {conf_path} \
#     {output_path} \
#     {hdfs_uri}"""

cmd = """export SPARK_MAJOR_VERSION=2; spark-submit \
   --name "carte_carto-1.0" \
   --master yarn \
   --num-executors 3  \
   --executor-memory 12g \
   --driver-memory  '4g'  \
   --class com.l******.App \
   'hdfs:/user/allieart/cartographie/carte_carto-1.0.jar' \
   '/user/allieart/cartographie/conf' \
   '/user/allieart/cartographie/output' \
   'hdfs://endor.l*****.local:8020'"""


t2 = SSHOperator(
    task_id = 'run_mapping_task',
    command = cmd,
    # command = create_spark_submit_cmd(job_name, jar_file, conf_path, output_path, hdfs_uri),
    ssh_conn_id = conn_id,
    dag = dag
)