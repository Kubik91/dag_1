from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    "pavel_kond_collaborative_dag",
    schedule_interval="@once",
    catchup=False,
    start_date=days_ago(2),
) as dag:

    bash_run_pyspark_task = BashOperator(
        task_id='bash_run_pyspark_task',
        bash_command = '''ssh shahidkubik@rc1b-dataproc-m-h8jzox1botuktl9j.mdb.yandexcloud.net spark-submit --master yarn --deploy-mode client --conf spark.sql.catalogImplementation=hive /home/shahidkubik/pavel_kond_collaborative.py'''
    )
