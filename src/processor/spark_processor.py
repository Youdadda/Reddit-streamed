#!/usr/bin/env python3

from dependencies.spark import start_spark

from utilities.transformations import raw_table, silver_table, gold_table
from utilities.ReadingSchema import posts_schema as schema
from utilities.ReadingSchema import comment_schema
from utilities.load import write_to_Table
from spark_instance import spark_sess


df_raw = raw_table(spark_sess)

df_silver = silver_table(df_raw) 

df_gold = gold_table(df_silver)


# query_b =  (
#     df_raw.writeStream
#     .foreachBatch(lambda batch_df, batch_id:
#                   write_to_Table(batch_df, "raw_table", batch_id))
#     .outputMode("update")
#     .start()
#     )
    
# query_s = (
#     df_silver.writeStream
#     .foreachBatch(lambda batch_df, batch_id:
#                   write_to_Table(batch_df, "silver_table", batch_id))
#     .outputMode("update")
#     .start()

# )
query_g = (
    df_gold.writeStream
    .foreachBatch(lambda batch_df, batch_id:
                  write_to_Table(batch_df, "gold_table", batch_id))
    .outputMode("update")
    .start()
)


query_g.awaitTermination()



"""
spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.app.name=KafkaDataProcessor" \
  --executor-memory 2g \
  --num-executors 3 \
  --conf spark.sql.streaming.concurrentJobs=3 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1\
  /processor/spark_processor.py



"""