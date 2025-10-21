import os 




def write_to_Table(df, table_name, batch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra" )\
        .option("keyspace", "reddit") \
        .option("table", table_name) \
        .mode("append") \
        .save()