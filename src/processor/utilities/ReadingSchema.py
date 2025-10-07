from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

comment_schema = StructType([
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", FloatType(), True)
])

posts_schema = StructType([
    StructField("category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("created_utc", FloatType(), True),
    StructField("post_body", StringType(), True),
    StructField("comments", ArrayType(comment_schema), True)
])