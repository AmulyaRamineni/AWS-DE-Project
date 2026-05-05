
"""
Glue Job: Statistics Data Transformation (Bronze → Silver Layer)
----------------------------------------------------------------

This AWS Glue job processes raw video statistics data from the Bronze layer
and transforms it into a clean, structured format in the Silver layer.

The job supports both:
- Historical Kaggle CSV datasets
- Live YouTube API JSON data

Key steps performed in this job:
- Reads raw data from the Bronze Glue catalog
- Detects input format (CSV vs API JSON)
- Applies schema standardization and type casting
- Cleans invalid or missing records
- Deduplicates repeated video entries
- Adds derived metrics (like ratio, engagement rate)
- Performs data quality checks
- Writes partitioned Parquet data to the Silver layer
- Updates the Glue Data Catalog for querying

Enhancements included:
- Handles multiple data formats seamlessly
- Deduplication based on video + region + date
- Standardized date parsing
- Incremental processing with predicate pushdown
- Structured logging and data quality validation

Job Parameters:
    --JOB_NAME           - Glue job name
    --bronze_database    - Bronze Glue database
    --bronze_table       - Bronze table name
    --silver_bucket      - Target S3 bucket
    --silver_database    - Silver Glue database
    --silver_table       - Silver table name
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, BooleanType
)

# ---------- Job Setup ----------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_table",
    "silver_bucket",
    "silver_database",
    "silver_table",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ---------- Configuration ----------
BRONZE_DB = args["bronze_database"]
BRONZE_TABLE = args["bronze_table"]

SILVER_BUCKET = args["silver_bucket"]
SILVER_DB = args["silver_database"]
SILVER_TABLE = args["silver_table"]

SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"

logger.info(f"Reading from Bronze: {BRONZE_DB}.{BRONZE_TABLE}")
logger.info(f"Writing to Silver: {SILVER_DB}.{SILVER_TABLE}")


# ---------- Step 1: Read Bronze ----------
logger.info("Loading data from Bronze layer...")

predicate = "region in ('ca','gb','us','in')"

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE,
    push_down_predicate=predicate,
)

df = datasource.toDF()
initial_count = df.count()

logger.info(f"Records loaded: {initial_count}")

if initial_count == 0:
    logger.info("No new data found. Skipping processing.")

else:
    # ---------- Step 2: Schema Handling ----------
    logger.info("Applying schema standardization...")

    columns = set(df.columns)

    # Detect API format vs CSV format
    if "snippet.title" in columns or "snippet__title" in columns:
        logger.info("Detected API JSON format")

        df = df.select(
            F.col("id").alias("video_id"),
            F.lit(datetime.utcnow().strftime("%y.%d.%m")).alias("trending_date"),
            F.col("`snippet.title`").alias("title") if "snippet.title" in columns else F.col("snippet__title").alias("title"),
            F.col("`snippet.channelTitle`").alias("channel_title") if "snippet.channelTitle" in columns else F.col("snippet__channelTitle").alias("channel_title"),
            F.col("`snippet.categoryId`").cast(LongType()).alias("category_id") if "snippet.categoryId" in columns else F.col("snippet__categoryId").cast(LongType()).alias("category_id"),
            F.col("`snippet.publishedAt`").alias("publish_time") if "snippet.publishedAt" in columns else F.col("snippet__publishedAt").alias("publish_time"),
            F.col("`statistics.viewCount`").cast(LongType()).alias("views") if "statistics.viewCount" in columns else F.col("statistics__viewCount").cast(LongType()).alias("views"),
            F.col("`statistics.likeCount`").cast(LongType()).alias("likes") if "statistics.likeCount" in columns else F.col("statistics__likeCount").cast(LongType()).alias("likes"),
            F.col("`statistics.commentCount`").cast(LongType()).alias("comment_count") if "statistics.commentCount" in columns else F.col("statistics__commentCount").cast(LongType()).alias("comment_count"),
            F.col("region"),
        )

    else:
        logger.info("Detected CSV format")

        df = df.select(
            F.col("video_id").cast(StringType()),
            F.col("trending_date").cast(StringType()),
            F.col("title").cast(StringType()),
            F.col("channel_title").cast(StringType()),
            F.col("category_id").cast(LongType()),
            F.col("publish_time").cast(StringType()),
            F.col("views").cast(LongType()),
            F.col("likes").cast(LongType()),
            F.col("comment_count").cast(LongType()),
            F.col("region").cast(StringType()),
        )


    # ---------- Step 3: Data Cleaning ----------
    logger.info("Cleaning data...")

    df = df.filter(F.col("video_id").isNotNull())
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

    df = df.withColumn(
        "trending_date_parsed",
        F.when(
            F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
            F.to_date(F.col("trending_date"), "yy.dd.MM")
        ).otherwise(F.to_date(F.col("trending_date")))
    )

    for col_name in ["views", "likes", "comment_count"]:
        df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    # Derived metrics
    df = df.withColumn(
        "like_ratio",
        F.when(F.col("views") > 0,
               F.round(F.col("likes") / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )

    df = df.withColumn(
        "engagement_rate",
        F.when(F.col("views") > 0,
               F.round((F.col("likes") + F.col("comment_count")) / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )

    df = df.withColumn("_processed_at", F.current_timestamp())
    df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))


    # ---------- Step 4: Deduplication ----------
    logger.info("Removing duplicate records...")

    from pyspark.sql.window import Window

    window = Window.partitionBy("video_id", "region", "trending_date_parsed") \
        .orderBy(F.col("_processed_at").desc())

    df = df.withColumn("row_num", F.row_number().over(window)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

    clean_count = df.count()
    logger.info(f"Clean records: {clean_count}")


    # ---------- Step 5: Data Quality Checks ----------
    logger.info("Running data quality checks...")

    for col_name in ["video_id", "title", "views"]:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            logger.warn(f"{col_name} has {null_count} null values")


    # ---------- Step 6: Write to Silver ----------
    logger.info("Writing data to Silver layer...")

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_data")

    sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
    )

    sink.setCatalogInfo(
        catalogDatabase=SILVER_DB,
        catalogTableName=SILVER_TABLE
    )

    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)

    logger.info(f"Write complete: {clean_count} records")

job.commit()
