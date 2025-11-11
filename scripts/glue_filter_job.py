# scripts/glue_filter_job.py
import sys
import argparse
import os
from etl.transform import filter_records

def run_local_csv(source_path, target_path, amount_threshold=600):
    # Simple local runner using CSV and pandas (works in CI and development)
    import csv
    records = []
    with open(source_path, newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            records.append(r)
    filtered = filter_records(records, amount_threshold)
    # write result
    fieldnames = filtered[0].keys() if filtered else ['id','name','amount']
    with open(target_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered)

def run_glue(glue_args):
    # This is the Glue/GPU job path. Use Spark if running on AWS Glue.
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    # expecting JOB_NAME, source_path, target_path
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])
    source = args['source_path']
    target = args['target_path']

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    df = spark.read.option("header", "true").csv(source)
    # transform using Spark API
    from pyspark.sql.functions import col
    filtered_df = df.filter(col("amount").cast("int") > int(glue_args.get('amount_threshold', 600)))
    filtered_df.write.mode("overwrite").option("header", "true").csv(target)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["local","glue"], default="local")
    parser.add_argument("--source", help="local source csv path")
    parser.add_argument("--target", help="local target csv path")
    parser.add_argument("--amount_threshold", type=int, default=600)
    args = parser.parse_args()

    if args.mode == "local":
        if not args.source or not args.target:
            print("Provide --source and --target for local mode")
            sys.exit(2)
        run_local_csv(args.source, args.target, args.amount_threshold)
    else:
        run_glue(vars(args))
