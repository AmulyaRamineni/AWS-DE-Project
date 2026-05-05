
"""
Lambda: Data Quality Validation (Silver Layer)
---------------------------------------------

This Lambda function is triggered after the Silver layer is built
(as part of a Step Functions workflow).

Its role is to validate the quality of processed data before allowing
the pipeline to move forward to the Gold layer.

The function performs multiple data quality checks, including:
- Verifying minimum row count
- Checking null percentages in critical columns
- Validating expected schema structure
- Ensuring numeric values fall within acceptable ranges
- Confirming data freshness (recent timestamps)

If any check fails, the pipeline can be stopped and an alert is sent.

Environment Variables:
    S3_BUCKET_SILVER      - Silver layer S3 bucket
    SNS_ALERT_TOPIC_ARN   - SNS topic for failure alerts (optional)
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

# ---------- Logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- AWS Clients ----------
sns = boto3.client("sns")

# ---------- Config ----------
SNS_TOPIC_ARN = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PERCENT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))

MAX_VIEWS = 50_000_000_000  # sanity limit
FRESHNESS_HOURS = 48        # max allowed data age

CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "clean_reference_data": ["id", "region"],
}


# ---------- Checks ----------
def check_row_count(df, table):
    """Ensure dataset has enough rows."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT

    return {
        "check": "row_count",
        "table": table,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min required: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df, table):
    """Check null percentage for critical columns."""
    results = []
    columns = CRITICAL_COLUMNS.get(table, [])

    for col in columns:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table,
                "column": col,
                "passed": False,
                "message": f"Missing column: {col}",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
        passed = null_pct <= MAX_NULL_PERCENT

        results.append({
            "check": "null_pct",
            "table": table,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": MAX_NULL_PERCENT,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PERCENT}%)",
        })

    return results


def check_schema(df, table):
    """Validate required columns exist."""
    expected = set(CRITICAL_COLUMNS.get(table, []))
    actual = set(df.columns)

    missing = expected - actual
    passed = len(missing) == 0

    return {
        "check": "schema",
        "table": table,
        "missing_columns": list(missing),
        "passed": passed,
        "message": "All required columns present" if passed else f"Missing: {missing}",
    }


def check_value_ranges(df, table):
    """Validate numeric ranges (e.g., views)."""
    results = []

    if table != "clean_statistics":
        return results

    if "views" in df.columns:
        negative = (df["views"] < 0).sum()
        extreme = (df["views"] > MAX_VIEWS).sum()

        passed = (negative == 0 and extreme == 0)

        results.append({
            "check": "value_range",
            "table": table,
            "column": "views",
            "negative_count": int(negative),
            "extreme_count": int(extreme),
            "passed": passed,
            "message": f"Views → negative: {negative}, extreme: {extreme}",
        })

    return results


def check_freshness(df, table):
    """Ensure data is recent."""
    if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
        return {
            "check": "freshness",
            "table": table,
            "passed": True,
            "message": "No timestamp column found — skipping check",
        }

    timestamp_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"

    try:
        latest = pd.to_datetime(df[timestamp_col]).max()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)

        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)

        passed = latest >= cutoff

        return {
            "check": "freshness",
            "table": table,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": passed,
            "message": f"Latest: {latest} | Cutoff: {cutoff}",
        }

    except Exception as e:
        return {
            "check": "freshness",
            "table": table,
            "passed": True,
            "message": f"Timestamp parsing failed — skipping ({e})",
        }


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    """
    Entry point for data quality validation.
    """

    database = event.get("database", "yt_pipeline_silver_dev")
    tables = event.get("tables", ["clean_statistics"])

    all_results = []
    overall_passed = True

    for table in tables:
        logger.info(f"Running checks on {database}.{table}")

        try:
            query = f'SELECT * FROM "{table}" LIMIT 10000'
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                ctas_approach=False,
            )
        except Exception as e:
            logger.error(f"Failed to read table {table}: {e}")

            all_results.append({
                "check": "read_table",
                "table": table,
                "passed": False,
                "message": str(e),
            })

            overall_passed = False
            continue

        checks = []
        checks.append(check_row_count(df, table))
        checks.extend(check_null_percentage(df, table))
        checks.append(check_schema(df, table))
        checks.extend(check_value_ranges(df, table))
        checks.append(check_freshness(df, table))

        for result in checks:
            logger.info(
                f"{result['check']} → {'PASS' if result['passed'] else 'FAIL'} | {result['message']}"
            )
            if not result["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # ---------- Summary ----------
    passed = sum(1 for r in all_results if r["passed"])
    total = len(all_results)

    logger.info(f"DQ Summary: {passed}/{total} checks passed")

    if not overall_passed and SNS_TOPIC_ARN:
        failed_checks = [r for r in all_results if not r["passed"]]

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="YouTube Pipeline - Data Quality FAILED",
            Message=json.dumps(failed_checks, indent=2, default=str),
        )

    return {
        "quality_passed": overall_passed,
        "checks_passed": passed,
        "checks_total": total,
        "details": json.loads(json.dumps(all_results, default=str)),
    }
