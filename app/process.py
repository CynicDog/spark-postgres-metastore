import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType,
    IntegerType, DateType, TimestampType
)


input_path = "/data/raw"

spark = SparkSession.builder \
    .appName("MetastoreProceduralTest") \
    .config("spark.sql.warehouse.dir", "/opt/spark/spark-warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("hive.stats.autogather", "false") \
    .config("spark.sql.statistics.fallBackToHdfs", "false") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS mta_db")
spark.sql("USE mta_db")

spark.sql("DROP TABLE IF EXISTS bronze_mta")
spark.sql("DROP TABLE IF EXISTS silver_mta")
spark.sql("DROP TABLE IF EXISTS gold_mta")

mta_schema = StructType([
    StructField('actual_track', StringType(), True),
    StructField('arrival_time', StringType(), True),
    StructField('departure_time', StringType(), True),
    StructField('direction', StringType(), True),
    StructField('ingestion_ts', StringType(), True),
    StructField('is_assigned', BooleanType(), True),
    StructField('route_id', StringType(), True),
    StructField('scheduled_track', StringType(), True),
    StructField('stop_id', StringType(), True),
    StructField('train_id', StringType(), True),
    StructField('trip_id', StringType(), True),
    StructField('date', DateType(), True),
    StructField('hour', IntegerType(), True)
])

raw_df = spark.read \
    .format("json") \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "*.jsonl") \
    .schema(mta_schema) \
    .load(input_path)

raw_df.write \
    .format("parquet") \
    .mode("append") \
    .saveAsTable("bronze_mta")

spark.sql("SHOW TABLES").show()

bronze_df = spark.table("bronze_mta")

silver_df = bronze_df \
    .withColumn("route_id", F.upper(F.col("route_id"))) \
    .filter(F.col("route_id").isNotNull())

silver_df.write \
    .format("parquet") \
    .mode("append") \
    .saveAsTable("silver_mta")

spark.sql("SHOW TABLES").show()

silver_df = spark.table("silver_mta")
gold_df = silver_df.groupBy("route_id").count()

gold_df.write \
    .format("parquet") \
    .mode("append") \
    .saveAsTable("gold_mta")

spark.sql("SHOW TABLES").show()

spark.table("gold_mta").show(20, truncate=False)

spark.sql("SHOW TABLES").show()
spark.stop()

import sys
sys.exit(0)