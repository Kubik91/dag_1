import gzip
import os
import shutil
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from os import remove
from urllib.request import urlopen, urlretrieve

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

default_args = {"start_date": datetime(2021, 11, 11)}

data_url = "https://storage.yandexcloud.net/misis-zo-bigdata"


def _set_keys(keys):
    Variable.set(key="list_of_keys", value=keys, serialize_json=True)
    print("Keys set")


def S3KeySensor():
    response = requests.get(f"{data_url}?list-type=2&encoding-type=url")
    xml = ET.fromstring(response.text)
    xmlns = xml.tag.replace("ListBucketResult", "")
    return bool(int(xml.find(f"{xmlns}KeyCount").text))


def test_data():
    urlretrieve(
        "http://deepyeti.ucsd.edu/jianmo/amazon/categoryFiles/All_Beauty.json.gz",
        "All_Beauty.json.gz",
    )
    with gzip.open("All_Beauty.json.gz", "rb") as f_in, open(
        "All_Beauty_1.json", "wb"
    ) as f_out_1, open("All_Beauty_2.json", "wb") as f_out_2:
        shutil.copyfileobj(f_in, f_out_1)
        shutil.copyfileobj(f_in, f_out_2)
    keys = ["All_Beauty_1", "All_Beauty_2"]
    for key in keys:
        with open(f"{key}.json", "r") as data:
            json2csv(data, key)
    os.remove("All_Beauty.json.gz")
    os.remove("All_Beauty_1.json")
    os.remove("All_Beauty_2.json")
    _set_keys(keys)


def json2csv(data, key):
    columns = [
        "overall",
        "verified",
        "reviewTime",
        "reviewerID",
        "asin",
        "reviewerName",
        "reviewText",
        "summary",
        "unixReviewTime",
    ]
    with open(f"~/tmp/{key}_all.json", "w") as jsonfile:
        for i, line in enumerate(data):
            if not i:
                print("[", file=jsonfile)
            else:
                print(",", file=jsonfile)
            print(line, file=jsonfile)
        else:
            print("]", file=jsonfile)
    df = pd.read_json(f"~/tmp/{key}_all.json", orient="records")
    df[columns].to_csv(f"~/tmp/{key}.csv")
    remove(f"~/tmp/{key}_all.json")


def load_data():
    response = requests.get(f"{data_url}?list-type=2&encoding-type=url")
    xml = ET.fromstring(response.text)
    xmlns = xml.tag.replace("ListBucketResult", "")
    keys = []
    for key in xml.find(f"{xmlns}Contents").findall(f"{xmlns}Key"):
        try:
            data = urlopen(f"{data_url}/{key.text}")
            json2csv(data, key.text.split(".")[0])
        except Exception as e:
            sys.stderr.write(f"ERROR: {e}")
        else:
            keys.append(key.text.split(".")[0])
    _set_keys(keys)


def _failure_callback(context):
    sys.stderr.write(f"ERROR: {context['exception']}")


with DAG(
    "pavel_dag", schedule_interval="0 * * * *", default_args=default_args, catchup=False
) as dag:
    s3_check = PythonSensor(
        task_id="S3KeySensor",
        poke_interval=120,
        timeout=30,
        mode="reschedule",
        python_callable=S3KeySensor,
        on_failure_callback=_failure_callback,
        soft_fail=True,
    )
    s3_test = PythonSensor(
        task_id="test_data",
        poke_interval=120,
        timeout=30,
        mode="reschedule",
        python_callable=test_data,
        on_failure_callback=_failure_callback,
        soft_fail=True,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        trigger_rule="none_failed_or_skipped",
    )

    copy_hdfs_task = BashOperator(
        task_id="copy_hdfs_task",
        bash_command="hadoop fs -copyFromLocal ~/tmp/ /user/shahidkubik/staging",
    )

    keys_list = Variable.get("list_of_keys", default_var=[], deserialize_json=True)
    print("List of keys updated")

    with TaskGroup(
        "dynamic_tasks_group_load",
        prefix_group_id=False,
    ) as dynamic_tasks_group_load:
        if keys_list:
            for index, key in enumerate(keys_list):

                create_temp_table_hql = """DROP TABLE IF EXISTS {{ params.table_name }};
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
                        LOCATION '/user/shahidkubik/staging/';"""

                update_all_raitings_hql = """INSERT INTO TABLE all_raitings
                        SELECT overall, verified, from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy-MM-dd') as reviewtime,
                        reviewerid, asin, reviewername, reviewtext, summary, unixreviewtime,
                        from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM {{ params.table_name }};"""

                update_user_scores_hql = """INSERT INTO TABLE user_scores SELECT reviewerid, asin, overall, reviewtime,
                        from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM {{ params.table_name }};"""

                update_reviews_hql = """INSERT INTO TABLE reviews SELECT reviewerid, reviewtext, overall, reviewtime,
                        from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM {{ params.table_name }};"""

                update_product_scores_hql = """INSERT INTO TABLE product_scores SELECT asin, overall, reviewtime,
                        from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM {{ params.table_name }};"""

                remove_temp_table_hql = (
                    """DROP TABLE IF EXISTS {{ params.table_name }};"""
                )

                create_temp_table = HiveOperator(
                    hql=create_temp_table_hql,
                    hive_cli_conn_id="hive_staging",
                    schema="pavel_kandratsionak",
                    hiveconf_jinja_translate=True,
                    task_id="drop_old_table",
                    dag=dag,
                )

                parquet_all_raitings = HiveOperator(
                    hql=update_all_raitings_hql,
                    hive_cli_conn_id="hive_staging",
                    schema="pavel_kandratsionak",
                    hiveconf_jinja_translate=True,
                    task_id=f"parquet_all_raitings_{key}",
                    params={"table_name": f"{key}_tmp"},
                    dag=dag,
                )

                parquet_scores = HiveOperator(
                    hql=update_user_scores_hql,
                    hive_cli_conn_id="hive_staging",
                    schema="pavel_kandratsionak",
                    hiveconf_jinja_translate=True,
                    task_id=f"parquet_scores_{key}",
                    params={"table_name": f"{key}_tmp"},
                    dag=dag,
                )

                parquet_reviews = HiveOperator(
                    hql=update_reviews_hql,
                    hive_cli_conn_id="hive_staging",
                    schema="pavel_kandratsionak",
                    hiveconf_jinja_translate=True,
                    task_id=f"parquet_reviews_{key}",
                    params={"table_name": f"{key}_tmp"},
                    dag=dag,
                )

                parquet_product_scores = HiveOperator(
                    hql=update_product_scores_hql,
                    hive_cli_conn_id="hive_staging",
                    schema="pavel_kandratsionak",
                    hiveconf_jinja_translate=True,
                    task_id=f"parquet_product_scores_{key}",
                    params={"table_name": f"{key}_tmp"},
                    dag=dag,
                )

                remove_temp_table = HiveOperator(
                    hql=remove_temp_table_hql,
                    hive_cli_conn_id="hive_staging",
                    schema="pavel_kandratsionak",
                    hiveconf_jinja_translate=True,
                    task_id=f"drop_old_table_{key}",
                    params={"table_name": f"{key}_tmp"},
                    dag=dag,
                )

                # TaskGroup level dependencies
                create_temp_table >> parquet_all_raitings >> parquet_scores >> parquet_reviews >> parquet_product_scores >> remove_temp_table

    with TaskGroup(
        "dynamic_tasks_group_drop_duplicates",
        prefix_group_id=False,
    ) as dynamic_tasks_group_drop_duplicates:

        drop_duplicates_hql = """INSERT OVERWRITE TABLE {{ params.table_name }} SELECT DISTINCT * FROM {{ params.table_name }};"""

        for table in ["all_raitings", "user_scores", "reviews", "product_scores"]:
            parquet_drop_duplicates = HiveOperator(
                hql=drop_duplicates_hql,
                hive_cli_conn_id="hive_staging",
                schema="pavel_kandratsionak",
                hiveconf_jinja_translate=True,
                task_id=f"parquet_drop_duplicates_{table}",
                params={"table_name": f"{table}"},
                dag=dag,
            )

        parquet_drop_duplicates

s3_test >> copy_hdfs_task >> dynamic_tasks_group_load >> dynamic_tasks_group_drop_duplicates
