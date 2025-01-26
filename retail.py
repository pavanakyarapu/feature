import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F

# Manually set args for testing if running locally or outside the AWS Glue console
args = {
    'JOB_NAME': 'my-glue-job',
    'source_bucket': 'raw-bucket-retail',  # Replace with your bucket name
    'source_path': '/retail_data/budget_allocation_fact/',  # Replace with your path to source data
    'iceberg_table_path': 'base-bucket-iceberg/my_iceberg/',  # Replace with your Iceberg table path
    'aws_region': 'us-east-1'  # AWS region
}

# Initialize the GlueContext and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a Glue Job instance
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create the database in Glue Catalog if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS my_database")

# Read the raw data from S3 (assuming it's in CSV format)
raw_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [f"s3://{args['source_bucket']}{args['source_path']}"],
        "recurse": True
    },
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# Convert DynamicFrame to DataFrame (Spark DataFrame)
df = raw_data.toDF()

# Example Transformation: Cast columns to appropriate types
df = df.withColumn(
    "budget_id",
    F.when(F.col("budget_id").rlike("^[0-9]+$"), F.col("budget_id").cast("int")).otherwise(F.lit(None).cast("int"))
)

df = df.withColumn(
    "store_id",
    F.when(F.col("store_id").rlike("^[0-9]+$"), F.col("store_id").cast("int")).otherwise(F.lit(None).cast("int"))
)

df = df.withColumn(
    "allocated_amount",
    F.when(F.col("allocated_amount").rlike("^[0-9]*\.?[0-9]+$"), F.col("allocated_amount").cast("decimal(10,2)")).otherwise(F.lit(None).cast("decimal(10,2)"))
)

# Cast 'start_date' and 'end_date' to Date type (assuming the format is 'yyyy-MM-dd')
df = df.withColumn(
    "start_date",
    F.to_date(F.col("start_date"), "yyyy-MM-dd").cast("date")
)

df = df.withColumn(
    "end_date",
    F.to_date(F.col("end_date"), "yyyy-MM-dd").cast("date")
)

# Example Transformation: Filter rows where 'store_id' is not null
df = df.filter(df['store_id'].isNotNull())

# Convert the transformed DataFrame back to DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")

# Set up the catalog configuration for Iceberg to use AWS Glue as the catalog
spark.conf.set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.my_catalog.type", "glue")  # Use Glue as the catalog type
spark.conf.set("spark.sql.catalog.my_catalog.warehouse", "s3://my-iceberg-bucket/warehouse/")  # Set the Iceberg warehouse location

# Create the Iceberg table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS my_catalog.my_database.my_iceberg_table (
        budget_id INT,
        store_id INT,
        allocated_amount DECIMAL(10, 2),
        start_date DATE,
        end_date DATE
    )
    USING iceberg
    LOCATION 's3://{args['iceberg_table_path']}'
""")

# Write the data from the DynamicFrame into the Iceberg table
transformed_dynamic_frame.toDF().write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("my_catalog.my_database.my_iceberg_table")

# Commit the Glue Job
job.commit()
