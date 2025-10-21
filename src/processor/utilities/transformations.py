from pyspark.sql import Row
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, ArrayType
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
    ### CASSANDRA DOESN'T SUPPORT NESTED TYPES SO WE NEED TO CONVERT THE COMMENTS ARRAY TO A JSON STRING
    df_raw = df.withColumn("comments", F.to_json(F.col("comments"))) \
                .withColumn("id", F.expr("uuid()"))

    return df_raw

## Silver Table

def silver_table(df):
    """
    Handle comments stored as a JSON string (as produced by raw_table).
    Steps:
      - Parse comments JSON back to array of structs
      - Filter out robot/welcome comments
      - Clean comment.body by removing links
      - Re-serialize comments array back to JSON string (Cassandra-compatible)
      - Convert created_utc to timestamp and drop created_utc
    """
    # parse comments JSON string -> array<struct(...)>
    df = df.withColumn("comments_arr", F.from_json(F.col("comments"), ArrayType(comment_schema)))

    # remove robot welcome comments
    df = df.withColumn(
        "comments_arr",
        F.expr("filter(comments_arr, item -> NOT startsWith(item.body, '\nWelcome to r/Morocco!'))")
    )

    # clean each comment body (remove links) and keep other fields
    df = df.withColumn(
        "comments_arr",
        F.transform(
            F.col("comments_arr"),
            lambda c: F.struct(
                F.regexp_replace(c["body"], r'http\\S+', '').alias("body"),
                *[c[f].alias(f) for f in comment_schema.names if f != "body"]
            )
        )
    )

    # convert back to JSON string for Cassandra
    df = df.withColumn("comments", F.to_json(F.col("comments_arr"))).drop("comments_arr")

    # convert created_utc to timestamp and drop the original field
    df = df.withColumn(
        "created",
        F.from_unixtime(F.col("created_utc")).cast("timestamp")
    ).drop("created_utc")

    return df

### Gold Table
## We aim to do some topic modeling so we're going to do the classic text cleaning methods,

def gold_table(df):

    df = df.withColumn(
        "cleaned_post_body",
        F.lower(col("post_body"))
    ) 
    tokenizer = RegexTokenizer(
    inputCol="cleaned_post_body",
    outputCol="tokens",
    pattern="\\W"
    )

    df = tokenizer.transform(df)

    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="cleaned_post_body_tokens"
    )
    stop_words = remover.getStopWords() + [
            "https", "co", "get", "like", "just", "people",
            "know", "time", "think", "see", "good", "make",
            "want", "need", "way", "even", "really"
        ]
    remover = remover.setStopWords(
        stop_words
    )

    df = remover.transform(df)

    # Create a literal column for the stop words list
    stop_words_literal = F.lit(stop_words)

    # --- Start of ETL Transformation ---

    # 3. Step 1: Convert the JSON string 'comments' back into an Array of Structs
    #    We create a temporary column named 'comments_arr' (the array)
    df_with_array = df.withColumn(
        "comments_arr",
        F.from_json(F.col("comments"), ArrayType(comment_schema))
    )

    # 4. Step 2: Perform the Stop Word Removal Transformation on the Array
    #    This is done on the 'comments_arr' column (the ARRAY type)
    df_cleaned = df_with_array.withColumn(
        "comments_arr_cleaned",
        F.transform(
            F.col("comments_arr"),  # Input is the ARRAY column
            lambda c: F.struct(
                # Keep original non-body fields
                c["score"].alias("score"),
                c["created_utc"].alias("created_utc"),

                # Clean 'body' and create 'tokens' field
                F.lower(c["body"]).alias("body"), # Lowercase body (for consistency/future use)
                
                F.filter(
                    # Split the body by non-word characters ('\W+')
                    F.split(
                        # First, replace 'https://...' with an empty string
                        F.regexp_replace(F.lower(c["body"]), r"http\S+", ""),
                        r"\W+"
                    ),
                    # Filter expression: token (t) is not empty AND stop_words_literal does NOT contain t
                    lambda t: (t != "") & (~F.array_contains(stop_words_literal, t))
                ).alias("tokens")
            )
        )
    )
    # The 'comments_arr_cleaned' column now holds the array of structs with the new 'tokens' field.

    # 5. Step 3: Convert the cleaned Array of Structs back into a JSON String for Cassandra
    df_final = df_cleaned.withColumn(
        "comments",  # Overwrite the original 'comments' column with the new JSON string
        F.to_json(F.col("comments_arr_cleaned"))
    )

    # 6. Step 4: Clean up temporary columns
    df_final = df_final.drop("comments_arr", "comments_arr_cleaned")
    # ---------------------------
    # end comments processing
    # ---------------------------

    df = df.drop("post_body").drop("url").drop("created").drop("tokens")
    return df
