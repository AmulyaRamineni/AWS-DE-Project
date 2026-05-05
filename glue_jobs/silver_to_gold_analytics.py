
"""
Glue Job: Analytics Aggregation (Silver → Gold Layer)
----------------------------------------------------

This Glue job reads cleaned data from the Silver layer and transforms it
into business-level aggregated datasets in the Gold layer.

The Gold layer is optimized for analytics, reporting, and dashboards
(e.g., Athena queries or QuickSight visualizations).

This job produces three main analytical tables:

1. trending_analytics   → Daily performance metrics by region
2. channel_analytics    → Channel-level performance insights
3. category_analytics   → Category trends over time

Key steps:
- Load statistics and optional reference data from Silver
- Enrich statistics with category names (if available)
- Aggregate metrics at multiple levels (region, channel, category)
- Write partitioned Parquet datasets to S3
- Update Glue Data Catalog for querying

Job Parameters:
    --JOB_NAME          - Glue job name
    --silver_database   - Silver Glue database
    --gold_bucket       - Target S3 bucket
    --gold_database     - Gold Glue database
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------- Job Setup ----------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "gold_bucket",
    "gold_database",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB = args["gold_database"]


# ---------- Step 1: Read Silver Data ----------
logger.info("Loading Silver layer data...")

stats_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name="clean_statistics",
)
stats_df = stats_dyf.toDF()

logger.info(f"Statistics records loaded: {stats_df.count()}")


# ---------- Step 2: Load Reference Data (Optional) ----------
logger.info("Loading reference data for category mapping...")

try:
    ref_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name="clean_reference_data",
    )
    ref_df = ref_dyf.toDF()

    category_lookup = None

    # Handle different schema formats from crawlers
    if "id" in ref_df.columns and "snippet.title" in ref_df.columns:
        category_lookup = ref_df.select(
            F.col("id").cast("long").alias("category_id"),
            F.col("`snippet.title`").alias("category_name"),
        ).dropDuplicates(["category_id"])

    elif "id" in ref_df.columns and "snippet_title" in ref_df.columns:
        category_lookup = ref_df.select(
            F.col("id").cast("long").alias("category_id"),
            F.col("snippet_title").alias("category_name"),
        ).dropDuplicates(["category_id"])

    else:
        logger.warn(f"Unexpected schema in reference data: {ref_df.columns}")

    # Join category names into statistics
    if category_lookup is not None:
        logger.info(f"Category lookup size: {category_lookup.count()}")

        if "category_id" in stats_df.columns:
            stats_df = stats_df.withColumn("category_id", F.col("category_id").cast("long"))

        stats_df = stats_df.join(
            F.broadcast(category_lookup),
            on="category_id",
            how="left",
        )

except Exception as e:
    logger.warn(f"Reference data not available: {e}")


# Ensure category_name column always exists
if "category_name" not in stats_df.columns:
    stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))
else:
    stats_df = stats_df.fillna("Unknown", subset=["category_name"])


# ═══════════════════════════════════════════════════════════════
# GOLD TABLE 1: Trending Analytics
# ═══════════════════════════════════════════════════════════════
logger.info("Building trending analytics table...")

trending_df = stats_df.groupBy("region", "trending_date_parsed").agg(
    F.count("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    # removed dislikes because column does not exist
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.avg("like_ratio").alias("avg_like_ratio"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.max("views").alias("max_views"),
    F.countDistinct("channel_title").alias("unique_channels"),
    F.countDistinct("category_id").alias("unique_categories"),
)

trending_df = trending_df.withColumn("_aggregated_at", F.current_timestamp())

trending_path = f"s3://{GOLD_BUCKET}/youtube/trending_analytics/"

sink_trending = glueContext.getSink(
    connection_type="s3",
    path=trending_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)

sink_trending.setCatalogInfo(
    catalogDatabase=GOLD_DB,
    catalogTableName="trending_analytics"
)

sink_trending.setFormat("glueparquet", compression="snappy")
sink_trending.writeFrame(DynamicFrame.fromDF(trending_df, glueContext, "trending"))

logger.info(f"Trending analytics written: {trending_df.count()} rows")


# ═══════════════════════════════════════════════════════════════
# GOLD TABLE 2: Channel Analytics
# ═══════════════════════════════════════════════════════════════
logger.info("Building channel analytics table...")

channel_df = stats_df.groupBy("channel_title", "region").agg(
    F.countDistinct("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.max("views").alias("peak_views"),
    F.count("trending_date_parsed").alias("times_trending"),
    F.min("trending_date_parsed").alias("first_trending"),
    F.max("trending_date_parsed").alias("last_trending"),
    F.collect_set("category_name").alias("categories"),
)

# Rank channels by total views per region
rank_window = Window.partitionBy("region").orderBy(F.col("total_views").desc())

channel_df = channel_df.withColumn("rank_in_region", F.row_number().over(rank_window))
channel_df = channel_df.withColumn("_aggregated_at", F.current_timestamp())

channel_path = f"s3://{GOLD_BUCKET}/youtube/channel_analytics/"

sink_channel = glueContext.getSink(
    connection_type="s3",
    path=channel_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)

sink_channel.setCatalogInfo(
    catalogDatabase=GOLD_DB,
    catalogTableName="channel_analytics"
)

sink_channel.setFormat("glueparquet", compression="snappy")
sink_channel.writeFrame(DynamicFrame.fromDF(channel_df, glueContext, "channel"))

logger.info(f"Channel analytics written: {channel_df.count()} rows")


# ═══════════════════════════════════════════════════════════════
# GOLD TABLE 3: Category Analytics
# ═══════════════════════════════════════════════════════════════
logger.info("Building category analytics table...")

category_df = stats_df.groupBy(
    "category_name", "category_id", "region", "trending_date_parsed"
).agg(
    F.count("video_id").alias("video_count"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.countDistinct("channel_title").alias("unique_channels"),
)

# Calculate share of views per region/day
total_window = Window.partitionBy("region", "trending_date_parsed")

category_df = category_df.withColumn(
    "view_share_pct",
    F.round(F.col("total_views") / F.sum("total_views").over(total_window) * 100, 2)
)

category_df = category_df.withColumn("_aggregated_at", F.current_timestamp())

category_path = f"s3://{GOLD_BUCKET}/youtube/category_analytics/"

sink_category = glueContext.getSink(
    connection_type="s3",
    path=category_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)

sink_category.setCatalogInfo(
    catalogDatabase=GOLD_DB,
    catalogTableName="category_analytics"
)

sink_category.setFormat("glueparquet", compression="snappy")
sink_category.writeFrame(DynamicFrame.fromDF(category_df, glueContext, "category"))

logger.info(f"Category analytics written: {category_df.count()} rows")


logger.info("Gold layer processing complete.")
job.commit()
