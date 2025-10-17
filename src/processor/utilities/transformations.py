from pyspark.sql import Row
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType
from utilities.ReadingSchema import posts_schema as schema
from utilities.ReadingSchema import comment_schema
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
#### Raw table

def raw_table(spark_session):
    df = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:29092") \
    .option("subscribe", "reddit") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

    return df

## Silver Table

def silver_table(df):
    # 1. Delete the robot comments 
    df_filtered = df.withColumn(
        "filtered_comments",
        F.expr("filter(comments, item -> NOT startsWith(item.body, '\nWelcome to r/Morocco!'))")
    )

    # 2. Next, use 'transform' on the filtered array to clean the text inside the 'body' field
    df_silver = df_filtered.withColumn(
        "comments",
        F.transform(
            F.col("filtered_comments"), 
            lambda c: F.struct(
                # delete links
                F.regexp_replace(c["body"], r'http\S+', '').alias("body"),
                # Keep all other fields (e.g., 'id', 'author', etc.)
            *[c[f].alias(f) for f in comment_schema.names if f != 'body'] 
            )
        )
    ).drop("filtered_comments")  
    df_silver = df_silver.withColumn(
            "created",
            F.from_unixtime(F.col("created_utc")).cast("timestamp")
    ).drop("created_utc").drop("comments")
    return df_silver

### Gold Table
## We aim to do some topic modeling so we're going to do the classic text cleaning methods,

def gold_table(df):

    df = df.withColumn(
        "Cleaned_post_body",
        F.lower(col("post_body"))
    ) 
    tokenizer = RegexTokenizer(
    inputCol="Cleaned_post_body",
    outputCol="tokens",
    pattern="\\W"
    )

    df = tokenizer.transform(df)

    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="Cleaned_post_body_tokens"
    )
    remover = remover.setStopWords(
        remover.getStopWords() + [
            "https", "co", "get", "like", "just", "people",
            "know", "time", "think", "see", "good", "make",
            "want", "need", "way", "even", "really"
        ]
    )

    df = remover.transform(df)

    df = df.drop("post_body").drop("url").drop("created").drop("tokens")
    return df
