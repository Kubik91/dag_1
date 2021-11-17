import logging
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from os import remove
from pathlib import Path
from urllib.request import urlopen

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

data_url = "https://storage.yandexcloud.net/misis-zo-bigdata"


def S3KeySensor():
    logging.info(f"Start loading: {data_url}?list-type=2&encoding-type=url")
    response = requests.get(f"{data_url}?list-type=2&encoding-type=url")
    xml = ET.fromstring(response.text)
    xmlns = xml.tag.replace("ListBucketResult", "")
    return bool(int(xml.find(f"{xmlns}KeyCount").text))


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
    Path("/tmp/pavel_kond/tmp/").mkdir(parents=True, exist_ok=True)
    with open(f"/tmp/pavel_kond/tmp/{key}_all.json", "w") as jsonfile:
        for i, line in enumerate(data):
            if not i:
                print("[", file=jsonfile)
            else:
                print(",", file=jsonfile)
            print(line.decode("utf-8"), file=jsonfile, end="")
        else:
            print("]", file=jsonfile)
    df = pd.read_json(f"/tmp/pavel_kond/tmp/{key}_all.json", orient="records")
    df.fillna("")[columns].to_csv(f"/tmp/pavel_kond/tmp/{key}.csv", index=False)
    remove(f"/tmp/pavel_kond/tmp/{key}_all.json")


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
            print(f"ERROR: {e}")
        else:
            keys.append(key.text.split(".")[0])
    return keys


def _failure_callback(context):
    sys.stderr.write(f"ERROR: {context['exception']}")


with DAG(
    "pk_dag", schedule_interval="0 * * * *", catchup=False, start_date=days_ago(2)
) as dag:
    s3_check_sensor = PythonSensor(
        task_id="S3KeySensor",
        poke_interval=120,
        timeout=30,
        mode="reschedule",
        python_callable=S3KeySensor,
        on_failure_callback=_failure_callback,
        soft_fail=True,
    )

    load_data_operator = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        trigger_rule="none_failed_or_skipped",
    )

    copy_hdfs_task_operator = BashOperator(
        task_id="copy_hdfs_task",
        bash_command="hadoop fs -rm -r /user/shahidkubik/amazon_reviews"
                     "&& hdfs dfs -mkdir -p /user/shahidkubik/amazon_reviews/staging/ "
                     "&& hadoop fs -put -f /tmp/pavel_kond/tmp/* /user/shahidkubik/amazon_reviews/staging "
                     "&& hdfs dfs -chmod -R 777 /user/shahidkubik/amazon_reviews/ "
                     "&& rm -r /tmp/pavel_kond",
    )

    with TaskGroup(
        "create_tables",
        prefix_group_id=False,
    ) as create_tables_group:

        create_all_raitings_hql = """
            CREATE TABLE IF NOT EXISTS all_raitings(
                overall numeric(2,1), 
                verified boolean, 
                reviewtime date, 
                reviewerid string, 
                asin string, 
                reviewername string, 
                reviewtext string, 
                summary string, 
                unixreviewtime int)
            PARTITIONED BY (part_year int)
            STORED AS PARQUET
            LOCATION '/user/shahidkubik/amazon_reviews/all_raitings';
        """

        create_user_scores_hql = """
            CREATE TABLE IF NOT EXISTS user_scores(
                reviewerid string, 
                asin string,
                overall numeric(2,1),
                reviewtime date)
            PARTITIONED BY (part_year int)
            STORED AS PARQUET
            LOCATION '/user/shahidkubik/amazon_reviews/user_scores';
        """

        create_reviews_hql = """
            CREATE TABLE IF NOT EXISTS reviews(
                reviewerid string, 
                reviewtext string,
                overall numeric(2,1),
                reviewtime date)
            PARTITIONED BY (part_year int)
            STORED AS PARQUET
            LOCATION '/user/shahidkubik/amazon_reviews/reviews';;
        """

        create_product_scores_hql = """
            CREATE TABLE IF NOT EXISTS product_scores(
                asin string, 
                overall numeric(2,1), 
                reviewtime date)
            PARTITIONED BY (part_year int)
            STORED AS PARQUET
            LOCATION '/user/shahidkubik/amazon_reviews/product_scores';;
        """

        create_all_raitings_table_operator = HiveOperator(
            hql=create_all_raitings_hql,
            hive_cli_conn_id="hive_staging",
            schema="pavel_kandratsionak",
            hiveconf_jinja_translate=True,
            task_id="create_all_raitings",
        )

        create_user_scores_table_operator = HiveOperator(
            hql=create_user_scores_hql,
            hive_cli_conn_id="hive_staging",
            schema="pavel_kandratsionak",
            hiveconf_jinja_translate=True,
            task_id="create_user_scores",
        )

        create_reviews_table_operator = HiveOperator(
            hql=create_reviews_hql,
            hive_cli_conn_id="hive_staging",
            schema="pavel_kandratsionak",
            hiveconf_jinja_translate=True,
            task_id="create_reviews",
        )

        create_product_scores_table_operator = HiveOperator(
            hql=create_product_scores_hql,
            hive_cli_conn_id="hive_staging",
            schema="pavel_kandratsionak",
            hiveconf_jinja_translate=True,
            task_id="create_product_scores",
        )

    create_temp_table_hql = """DROP TABLE IF EXISTS data_temp;
            CREATE EXTERNAL TABLE data_temp (
                                    overall numeric(2,1),
                                    verified boolean,
                                    reviewtime string,
                                    reviewerid string,
                                    asin string,
                                    reviewername string,
                                    reviewtext string,
                                    summary string,
                                    unixreviewtime int)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
            STORED AS TEXTFILE
            LOCATION '/user/shahidkubik/staging/';"""

    create_temp_table_operator = HiveOperator(
        hql=create_temp_table_hql,
        hive_cli_conn_id="hive_staging",
        schema="pavel_kandratsionak",
        hiveconf_jinja_translate=True,
        task_id="create_temp_table",
    )

    test_temp_hql = """SELECT * FROM data_temp LIMIT 5;"""

    test_temp_table_operator = HiveOperator(
        hql=test_temp_hql,
        hive_cli_conn_id="hive_staging",
        schema="pavel_kandratsionak",
        hiveconf_jinja_translate=True,
        task_id="test_temp_table",
    )

    with TaskGroup(
        "update_tables_group",
        prefix_group_id=False,
    ) as update_tables_group:

        update_tables_hql = {
            "update_all_raitings_hql": """INSERT INTO TABLE all_raitings
                    SELECT overall, verified, from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy-MM-dd') as reviewtime,
                    reviewerid, asin, reviewername, reviewtext, summary, unixreviewtime,
                    from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_temp;""",
            "update_user_scores_hql": """INSERT INTO TABLE user_scores SELECT reviewerid, asin, overall, reviewtime,
                    from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_temp;""",
            "update_reviews_hql": """INSERT INTO TABLE reviews SELECT reviewerid, reviewtext, overall, reviewtime,
                    from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_temp;""",
            "update_product_scores_hql": """INSERT INTO TABLE product_scores SELECT asin, overall, reviewtime,
                    from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_temp;""",
        }

        for table in ["all_raitings", "user_scores", "reviews", "product_scores"]:

            parquet_table_operator = HiveOperator(
                hql=update_tables_hql.get(f"update_{table}_hql"),
                hive_cli_conn_id="hive_staging",
                schema="pavel_kandratsionak",
                hiveconf_jinja_translate=True,
                task_id=f"parquet_{table}",
            )

    remove_temp_table_hql = """DROP TABLE IF EXISTS data_temp;"""

    remove_temp_table_operator = HiveOperator(
        hql=remove_temp_table_hql,
        hive_cli_conn_id="hive_staging",
        schema="pavel_kandratsionak",
        hiveconf_jinja_translate=True,
        task_id="remove_temp_table",
    )

    remove_temp_files_operator = BashOperator(
        task_id="remove_temp_files",
        bash_command="hadoop fs -rm -r /user/shahidkubik/amazon_reviews/staging",
    )

    with TaskGroup(
        "drop_duplicates_group",
        prefix_group_id=False,
    ) as drop_duplicates_group:

        drop_duplicates_hql = """INSERT OVERWRITE TABLE {{ params.table_name }} SELECT DISTINCT * FROM {{ params.table_name }};"""

        for table in ["all_raitings", "user_scores", "reviews", "product_scores"]:

            create_all_raitings_hql = """
                CREATE TABLE IF NOT EXISTS user_scores(
                    reviewerid string, 
                    asin string,
                    overall numeric(2,1),
                    reviewtime date)
                PARTITIONED BY (part_year int)
                STORED AS PARQUET
                LOCATION '/user/shahidkubik/amazon_reviews/user_scores';
                """
            parquet_drop_duplicates = HiveOperator(
                hql=drop_duplicates_hql,
                hive_cli_conn_id="hive_staging",
                schema="pavel_kandratsionak",
                hiveconf_jinja_translate=True,
                task_id=f"parquet_drop_duplicates_{table}",
                params={"table_name": f"{table}"},
            )

    with TaskGroup(
        "test_group",
        prefix_group_id=False,
    ) as test_group:

        test_hql = """SELECT * FROM {{ params.table_name }} LIMIT 5;"""

        for table in ["all_raitings", "user_scores", "reviews", "product_scores"]:
            test_tebles = HiveOperator(
                hql=test_hql,
                hive_cli_conn_id="hive_staging",
                schema="pavel_kandratsionak",
                hiveconf_jinja_translate=True,
                task_id=f"test_{table}",
                params={"table_name": f"{table}"},
            )

s3_check_sensor >> load_data_operator >> copy_hdfs_task_operator >> create_tables_group >> create_temp_table_operator >> test_temp_table_operator >> update_tables_group >> remove_temp_table_operator >> remove_temp_files_operator >> drop_duplicates_group >> test_group
