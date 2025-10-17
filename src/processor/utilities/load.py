import os 




def write_to_Table(df, table_name, batch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", os.environ.get("CASSANDRA_HOST") )\
        .option("checkpointLocation", os.environ.get("CHECKPOINT_LOCATION")) \
        .option("keyspace", os.environ.get('KEYSPACE')) \
        .option("table", table_name) \
        .mode("append") \
        .save()