from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime

import requests
import xml.etree.ElementTree as ET
import pandas as pd

from urllib.request import urlopen

import gzip
import shutil

from os import remove
from urllib.request import urlretrieve

default_args = {'start_date': datetime(2021, 11, 11)}

data_url = 'https://storage.yandexcloud.net/misis-zo-bigdata'

def S3KeySensor():
    response = requests.get(f"{data_url}?list-type=2&encoding-type=url")
    xml = ET.fromstring(response.text)
    xmlns = xml.tag.replace("ListBucketResult", "")
    return bool(int(xml.find(f"{xmlns}KeyCount").text))

def test_data():
    urlretrieve("http://deepyeti.ucsd.edu/jianmo/amazon/categoryFiles/All_Beauty.json.gz", "All_Beauty.json.gz")
    with gzip.open('All_Beauty.json.gz', 'rb') as f_in:
    with open('All_Beauty.json', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def json2csv(data, key):
    with open(f"/tmp/{key}_all.json", "w") as jsonfile:
        for i, line in enumerate(data):
            if not i:
                print("[", file=jsonfile)
            else:
                print(",", file=jsonfile)
            print(line, file=jsonfile)
        else:
            print("]", file=jsonfile)
    df = pd.read_json(f"/tmp/{key}_all.json", orient='records')
    df.to_csv(f"/tmp/pavel_kond/{key}.csv")
    remove(f"/tmp/{key}_all.json")

def load_data_to_hive():
    response = requests.get(f"{data_url}?list-type=2&encoding-type=url")
    xml = ET.fromstring(response.text)
    xmlns = xml.tag.replace("ListBucketResult", "")
    for key in xml.find(f"{xmlns}Contents").findall(f"{xmlns}Key"):
        data = urlopen(f"{data_url}/{key}")
        json2csv(data, key)

def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")

with DAG('kond_dag', schedule_interval='0 * * * *', default_args=default_args, catchup=False) as dag:
    # s3_check = PythonSensor(
    #     task_id='S3KeySensor',
    #     poke_interval=120,
    #     timeout=30,
    #     mode="reschedule",
    #     python_callable=S3KeySensor,
    #     on_failure_callback=_failure_callback,
    #     soft_fail=True
    # )
    s3_test = PythonSensor(
        task_id='test_data',
        poke_interval=120,
        timeout=30,
        mode="reschedule",
        python_callable=test_data,
        on_failure_callback=_failure_callback,
        soft_fail=True
    )

    # json2csv = PythonOperator(
    #     task_id="json2csv",
    #     python_callable=S3Json2CSV,
    #     trigger_rule='none_failed_or_skipped'
    # )

    copy_hdfs_task = BashOperator(
        task_id='copy_hdfs_task',
        bash_command='hadoop fs -copyFromLocal ~/tmp/'
    )

    hql = """DROP TABLE IF EXISTS data_tmp;
            CREATE EXTERNAL TABLE data_tmp (
	                            overall numeric(2,1),
	                            verified boolean,
	                            reviewtime string,
                              reviewerid string,
                              asin string,
                              reviewername string,
                              reviewtext string,
                              summary string,
                              unixreviewtime int)
            ROW FORMAT delimited fields terminated by ','
            STORED AS TEXTFILE
            LOCATION '/user/dauren_naipov/staging/';"""

    hql1 = """INSERT INTO TABLE all_raitings
            SELECT overall, verified, from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy-MM-dd') as reviewtime,
            reviewerid, asin, reviewername, reviewtext, summary, unixreviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""


    hql2 = """INSERT INTO TABLE user_scores SELECT reviewerid, asin, overall, reviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""

    hql3 = """INSERT INTO TABLE reviews SELECT reviewerid, reviewtext, overall, reviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""

    hql4 = """INSERT INTO TABLE product_scores SELECT asin, overall, reviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""
   

    hive_load = HiveOperator(
        hql=hql,
        hive_cli_conn_id='hive_staging',
        schema='pavel_kandratsionak',
        hiveconf_jinja_translate=True,
        task_id='hive_load',
        dag=dag)

    parquet_all_raitings = HiveOperator(
        hql=hql1,
        hive_cli_conn_id='hive_staging',
        schema='pavel_kandratsionak',
        hiveconf_jinja_translate=True,
        task_id='parquet_all_raitings',
        dag=dag)

    parquet_scores = HiveOperator(
        hql=hql2,
        hive_cli_conn_id='hive_staging',
        schema='pavel_kandratsionak',
        hiveconf_jinja_translate=True,
        task_id='parquet_scores',
        dag=dag)

    parquet_reviews = HiveOperator(
        hql=hql3,
        hive_cli_conn_id='hive_staging',
        schema='pavel_kandratsionak',
        hiveconf_jinja_translate=True,
        task_id='parquet_reviews',
        dag=dag)

    parquet_product_scores = HiveOperator(
        hql=hql4,
        hive_cli_conn_id='hive_staging',
        schema='pavel_kandratsionak',
        hiveconf_jinja_translate=True,
        task_id='parquet_product_scores',
        dag=dag)

s3_check >> copy_hdfs_task >> hive_load >> parquet_all_raitings >> parquet_scores >> parquet_reviews >> parquet_product_scores
