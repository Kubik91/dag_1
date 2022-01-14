# spark-submit --master yarn --deploy-mode cluster --conf spark.sql.catalogImplementation=hive pavel_kond_collaborative.py
# DEBUG spark-submit --master yarn --deploy-mode client --conf spark.sql.catalogImplementation=hive pavel_kond_collaborative.py

import os
import subprocess

from pyspark import SparkConf, SparkContext
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, round, row_number
from pyspark.sql.utils import AnalysisException

os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"]
# spark.sparkContext.stop()
# conf = SparkConf().setAppName('pavel_kandratsionak - Collaborative filtering - python')
# spark = SparkSession.builder.config(conf=conf).getOrCreate()


if __name__ == "__main__":
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName(
        "pavel_kandratsionak - Collaborative filtering - python"
    )
    # conf.set(key="spark.kryoserializer.buffer.max", value="2048m")
    conf.set(key="spark.sql.shuffle.partitions", value="100")
    # conf.set(key="spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", value=True)
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    if not spark.catalog._jcatalog.tableExists("pavel_kandratsionak.user_scores_tmp"):
        spark.sql(
            """CREATE EXTERNAL TABLE IF NOT EXISTS pavel_kandratsionak.user_scores_tmp
                        (userid string, itemid string, rating string, timestamp_ string)
                            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                            STORED AS TEXTFILE
                            LOCATION '/user/root/ratings';"""
        )

    if not spark.catalog._jcatalog.tableExists(
        "pavel_kandratsionak.user_scores_collaborative"
    ):
        spark.sql(
            """CREATE TABLE IF NOT EXISTS pavel_kandratsionak.user_scores_collaborative (
                        userid string, 
                        itemid string,
                        rating string,
                        timestamp_ string)
                        PARTITIONED BY (year int)
                        STORED AS PARQUET;"""
        )

        spark.sql(
            """INSERT INTO TABLE pavel_kandratsionak.user_scores_collaborative
                    SELECT u.*, YEAR(from_unixtime(cast(u.timestamp_ as int))) as year
                    FROM pavel_kandratsionak.user_scores_tmp as u;"""
        )

    if not spark.catalog._jcatalog.tableExists(
        "pavel_kandratsionak.user_recommendations"
    ):
        print("--------------Get data--------------")
        data = spark.sql("select * from pavel_kandratsionak.user_scores_collaborative")

        print("--------------Get user ids--------------")
        uniq_userid = data.select("userid").sort("userid").distinct().sort("userid")

        uid_rdd = uniq_userid.rdd.map(lambda x: x["userid"]).zipWithIndex()
        uid_map = spark.createDataFrame(uid_rdd).toDF("userid", "userid_id")

        print("--------------Get item ids--------------")
        uniq_itemid = data.select("itemid").sort("itemid").distinct().sort("itemid")

        iid_rdd = uniq_itemid.rdd.map(lambda x: x["itemid"]).zipWithIndex()
        iid_map = spark.createDataFrame(iid_rdd).toDF("itemid", "itemid_id")

        print("--------------Get DataFrame--------------")
        df = data.join(uid_map, on="userid", how="left").join(
            iid_map, on="itemid", how="left"
        )

        df = df.withColumn("rating", col("rating").cast("double")).drop(
            "timestamp_", "year"
        )

        print("--------------Create model--------------")
        als = ALS(
            maxIter=5,
            regParam=0.01,
            rank=50,
            userCol="userid_id",
            itemCol="itemid_id",
            ratingCol="rating",
            nonnegative=True,
            implicitPrefs=False,
            coldStartStrategy="drop",
        )

        print("--------------Fit model--------------")
        model = als.fit(df)

        print("--------------Get recommendations--------------")
        recommendations = model.recommendForAllUsers(30)

        print("--------------Clear recommendations--------------")
        recommendations = recommendations.withColumn(
            "rec_exp", explode("recommendations")
        ).select("userid_id", col("rec_exp.itemid_id"), col("rec_exp.rating"))

        recommendations = recommendations.withColumn(
            "itemid_id", recommendations["itemid_id"].cast("double")
        )

        recommendations = (
            recommendations.join(
                df.drop("rating"), on=["userid_id", "itemid_id"], how="left"
            )
            .where(col("userid").isNull())
            .select("itemid_id", "userid_id", "rating")
            .join(uid_map, on="userid_id", how="left")
            .join(iid_map, on="itemid_id", how="left")
        )

        recommendations = recommendations.withColumn("rating", round("rating")).select(
            "userid", "itemid", "rating"
        )
        print("--------------Save recommendations--------------")
        try:
            recommendations.write.csv("/tmp/pavel_kandratsionak_recommendations")
            # recommendations.write.mode("overwrite").saveAsTable(
            #     "pavel_kandratsionak.user_recommendations"
            # )
        except AnalysisException as e:
            print("===================")
            subprocess.check_output(
                "hdfs dfs -rm -r -f /user/hive/warehouse/pavel_kandratsionak.db/user_recommendations",
                shell=True)
            recommendations.write.csv("/tmp/pavel_kandratsionak_recommendations")
            # recommendations.write.mode("overwrite").saveAsTable(
            #     "pavel_kandratsionak.user_recommendations"
            # )
        # pyspark.sql.utils.AnalysisException

    spark.sql("SELECT * FROM pavel_kandratsionak.user_recommendations").show()
