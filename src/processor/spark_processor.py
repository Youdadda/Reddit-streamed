#!/usr/bin/env python3

from dependencies.spark import start_spark
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from utilities.transformations import raw_table, silver_table, gold_table
from utilities.ReadingSchema import posts_schema as schema
from utilities.ReadingSchema import comment_schema


jar_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
]

spark_sess , spark_logger = start_spark(jar_packages=jar_packages)

df_raw = raw_table(spark_sess)

df_silver = silver_table(df_raw) 

df_gold = gold_table(df_silver)


query_bronze = df_raw.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() 

query = df_gold.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()



query.awaitTermination()


"""
spark-submit \
  --master spark:spark-master:7077 \
  --conf "spark.app.name=KafkaDataProcessor" \
  --executor-memory 2g \
  --num-executors 3 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /processor/spark_processor.py



"""