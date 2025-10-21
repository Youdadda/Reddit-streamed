from dependencies.spark import start_spark
jar_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.3"
]

spark_sess , spark_logger = start_spark(jar_packages=jar_packages)