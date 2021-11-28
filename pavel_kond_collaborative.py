# spark-submit --master yarn --deploy-mode cluster --conf spark.sql.catalogImplementation=hive pavel_kond_collaborative.py
# DEBUG spark-submit --master yarn --deploy-mode client --conf spark.sql.catalogImplementation=hive pavel_kond_collaborative.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
from pyspark.sql.functions import explode, col

if __name__ == "__main__":
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName('pavel_kandratsionak - Collaborative filtering - python')
    # conf.set(key="spark.kryoserializer.buffer.max", value="2048m")
    conf.set(key="spark.sql.shuffle.partitions", value="100")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    if not spark.catalog._jcatalog.tableExists('pavel_kandratsionak.user_scores_tmp'):
        spark.sql('''CREATE EXTERNAL TABLE IF NOT EXISTS pavel_kandratsionak.user_scores_tmp
                        (userid string, itemid string, rating string, timestamp_ string)
                            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                            STORED AS TEXTFILE
                            LOCATION '/user/shahidkubik/ratings';''')

    if not spark.catalog._jcatalog.tableExists('pavel_kandratsionak.user_scores_collaborative'):
        spark.sql('''CREATE TABLE IF NOT EXISTS pavel_kandratsionak.user_scores_collaborative (
                        userid string, 
                        itemid string,
                        rating string,
                        timestamp_ string)
                        PARTITIONED BY (year int)
                        STORED AS PARQUET;''')

        spark.sql('''INSERT INTO TABLE pavel_kandratsionak.user_scores_collaborative
                    SELECT u.*, YEAR(from_unixtime(cast(u.timestamp_ as int))) as year
                    FROM pavel_kandratsionak.user_scores_tmp as u;''')

    if not spark.catalog._jcatalog.tableExists('pavel_kandratsionak.user_recommendations'):
        data = spark.sql("select * from pavel_kandratsionak.user_scores_collaborative")

        indexers = (StringIndexer(inputCol=column, outputCol=f"{column}_id").fit(data) for column in
                    ["userid", "itemid"])

        pipeline = Pipeline(stages=indexers)

        df = pipeline.fit(data).transform(data)

        data = data.\
            withColumn('rating', col('rating').cast('double')).\
            drop('timestamp')

        als = ALS(
            maxIter=5,
            regParam=0.01,
            rank=50,
            userCol="userid_id",
            itemCol="itemid_id",
            ratingCol="rating",
            nonnegative=True,
            implicitPrefs=False,
            coldStartStrategy="drop"
        )

        model = als.fit(data)

        recommendations = model.recommendForAllUsers(30)
        recommendations = recommendations.withColumn('rec_exp', explode('recommendations')) \
            .select('userid_id', col('rec_exp.itemid'), col('rec_exp.rating'))

        recommendations = recommendations.withColumn("itemid_id", recommendations["itemid_id"].cast("double"))
        recommendations.join(df.drop("rating"), on=["userid_id", "itemid_id"], how="left").select("userid", "itemid", "rating")\
            .write.saveAsTable("pavel_kandratsionak.user_recommendations")

    spark.sql('SELECT * FROM pavel_kandratsionak.user_recommendations').show()
