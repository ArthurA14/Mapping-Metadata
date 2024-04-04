import airflow
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators import DummyOperator
import pendulum
import subprocess
from airflow.models import Variable

local_tz = pendulum.timezone("Europe/Paris")
sftp_password = Variable.get("ex*********")

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 6, tzinfo = local_tz),
    'email': ['c*****-de***n@g******.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


dag = DAG(
    "cartographie",
    catchup=False,
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=24),
    schedule_interval = '0 10 * * 1'
)

def run_script(project) : 
    return f'''
    spark-submit \
        --name "Cartographie" \
        --master yarn \
        --num-executors 4 \
        --executor-memory 25g \
        --driver-memory 25g \
        --conf spark.shuffle.service.enabled=true --conf spark.speculation=false \
        --conf spark.sql.parquet.fs.optimized.committer.optimization-enabled=true \
        --class com.l******.App \
        '/var/jar/carte_carto-1.0.jar' \
        '/projects/{project}/data/clean' \
        'hdfs://bt***.l*****.local:8020'
        '''

projects = ["b****","b*****","b****","b*****","c*****","d*****","gr*****","gr*****",
            "l*****","m*****","n******","o*****","o******","s*****","th*****","s*****"] 

Launcher = DummyOperator(task_id='Launcher',dag = dag)

for project in projects:

    cartographie = SSHOperator(
        task_id = f'cartographie_{project}',
        retries = 2,
        retry_delay = timedelta(seconds=5),
        command = run_script(project),
        ssh_conn_id = f'ssh_{project}_prod',
        dag = dag
    )
    Launcher >> cartographie