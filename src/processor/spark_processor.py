#!/usr/bin/env python3

from dependencies.spark import start_spark
from utilities.transformations import raw_table, silver_table, gold_table
from utilities.ReadingSchema import posts_schema as schema
from utilities.ReadingSchema import comment_schema
from utilities.load import write_to_Table
from spark_instance import spark_sess
import threading
import time

# Enable concurrent streaming jobs
spark_sess.conf.set("spark.sql.streaming.concurrentJobs", "3")

# Prepare the DataFrames
df_raw = raw_table(spark_sess)
df_silver = silver_table(df_raw) 
df_gold = gold_table(df_silver)

# Define query starts
def start_raw_query():
    query = df_raw.writeStream \
        .foreachBatch(lambda batch_df, batch_id:
                      write_to_Table(batch_df, "raw_table", batch_id)) \
        .outputMode("update") \
        .start()
    return query

def start_silver_query():
    query = df_silver.writeStream \
        .foreachBatch(lambda batch_df, batch_id:
                      write_to_Table(batch_df, "silver_table", batch_id)) \
        .outputMode("update") \
        .start()
    return query

def start_gold_query():
    query = df_gold.writeStream \
        .foreachBatch(lambda batch_df, batch_id:
                      write_to_Table(batch_df, "gold_table", batch_id)) \
        .outputMode("update") \
        .start()
    return query

# Start all queries in separate threads
queries = []
threads = []

for query_func in [start_raw_query, start_silver_query, start_gold_query]:
    query = query_func()
    queries.append(query)
    thread = threading.Thread(target=query.awaitTermination)
    threads.append(thread)
    thread.start()

# Monitor queries and handle termination
try:
    while any(query.isActive for query in queries):
        time.sleep(1)
        # Check for errors
        for query in queries:
            if query.exception():
                raise query.exception()
except KeyboardInterrupt:
    print("Stopping all queries...")
    for query in queries:
        query.stop()
finally:
    for thread in threads:
        thread.join()