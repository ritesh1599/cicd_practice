import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

#Added this comment for experiment1

# Get parameters passed from Glue Job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Read raw data (CSV)
df = spark.read.option("header", "true").csv(args['source_path'])

# Transformation: filter records where amount > 600
filtered_df = df.filter(df.amount.cast("int") > 600)

# Write to target (silver layer)
filtered_df.write.mode("overwrite").option("header", "true").csv(args['target_path'])

print("✅ Glue Job completed successfully.")
