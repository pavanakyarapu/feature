import sys
import logging
import boto3
import json
import hashlib
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.conf import SparkConf
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import udf, col, concat, lit, md5

# Setting up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Connecting to boto3 client
s3 = boto3.client('s3')

# Setting up CloudWatch logging
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# Getting values from parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'files', 'bucket'])
logBucketName = args['bucket'].replace("raw", "bas")
glueCatalogId = logBucketName.split('-')[-4]

# Setting Spark configuration
conf = SparkConf()
    .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("hive.metastore.glue.catalogid", glueCatalogId) \
    .set("spark.sql.warehouse.dir", f"s3://{logBucketName}") \
    .set("spark.sql.catalog.icebergCatalog.warehouse", f"s3://{logBucketName}") \
    .set("spark.sql.catalog.icebergCatalog", "org.apache.iceberg.spark.SparkCatalog") \
    .set("spark.sql.catalog.icebergCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .set("spark.sql.catalog.icebergCatalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

# Initialize GlueContext and Spark session
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initializing global variables
bucket_nm = args['bucket']
file_nm = args['files']
source_path = f"s3a://{bucket_nm}/{file_nm}"
tb_nm = file_nm.split('-')[1]
CATALOG_NAME = "icebergCatalog"
DB_NAME = "dpiinv_iceberg_bigdv1_fnzdb"


try:
    # Reading the schema file from S3
    response = s3.get_object(Bucket=bucket_nm, Key="data/fnz/scripts/glue_scripts/schema.json")
    content = response['Body']
    json_content = json.loads(content.read())
    logger.info("Schema JSON: %s", json_content)
    tb_columns = json_content['balance']['column_names']
    tb_schema = json_content['balance']['schema']

    logger.info(f"tb_columns: {tb_columns}")
    logger.info(f"tb_schema: {tb_schema}")
# Function to parse schema
    def parse_schema(schema_list):
        parsed_schema = []
        col_name = ""
        data_type_str = ""
        data_type = StringType()
        
        for schema in schema_list:
            parts = schema.split(" as ")
            col_name = parts[0].strip()
            data_type_str = parts[1].strip().lower()

            if "string" in data_type_str:
                data_type = StringType()
            elif "integer" in data_type_str:
                data_type = IntegerType()
            elif "decimal" in data_type_str:
                data_type = DecimalType(20, 8)
            else:
                data_type = StringType()

            parsed_schema.append(StructField(col_name, data_type, True))
        return StructType(parsed_schema)

    parsed_schema = parse_schema(tb_schema)

    # Reading the dataframe from S3
    df = spark.read.format("csv") \
                .option("header", "false") \
                .option("inferSchema", "false") \
                .option("delimiter", ",") \
                .schema(parsed_schema) \
                .load(source_path)

    df = df.toDF(*(tb_columns))
    logger.info(df.printSchema())

    # Validating columns
    logger.info(f"Source DataFrame columns: %s", df.columns)
    logger.info(f"Number of CSV columns: %d", len(df.columns))
    logger.info(f"Number of expected columns: %d", len(tb_columns))

    if len(df.columns) != len(tb_columns):
        raise Exception("Data file columns and schema columns are not matching")

    # Adding audit columns
    final_df = df.withColumn("effective_start_date", current_timestamp().cast(TimestampType()))\
        .withColumn("effective_end_date", lit("Null").cast(TimestampType()))) \
        .withColumn("current_flag", lit("Y").cast(stringType())) \
        .withColumn("balance_uid", md5(concat(col('effectivedate'), lit("|"), 
        col("subaccountid"), lit("|"), col("instrumentcode"), 
        lit("|"), col("location"), lit("|"), col("custodianaccountid"), lit("|"), 
        col("sourcesystemid"), lit("|"), col("positiontypeid"))))
    logger.info("DataFrame with SCD Type  Columns:")
    logger.info(f"Number of rows in final_df outside if statement: {final_df.count()}")

    final_df.createOrReplaceTempView("tb_nm")

# Get list of tables in the target database
    db_tables = spark.catalog.listTables(f"{CATALOG_NAME}.{DB_NAME}")
    db_table_list = [table.name for table in db_tables]
    logger.info("########### TARGET TABLE ###########")

# Check if table exists
    if tb_nm not in db_table_list:
        logger.info("Target table does not exist. Creating table with SCD Type 2 columns.")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.{tb_nm}
            USING iceberg
            TBLPROPERTIES ("format-version"="2")
            AS SELECT * FROM {tb_nm}
        """)
    else:
        target_df = spark.read.format("iceberg").table(f"{CATALOG_NAME}.{DB_NAME}.{tb_nm}")

        # Finding the matching records where key matches and current_flag is "Y"
        target_df_active = target_df.filter("current_flag" = "Y").alias("target")
        target_df_active = target_df_active.withColumn("balance_uid", md5(concat(col("effectivedate"), lit("|"),
        col("subaccountid"), lit("|"),col("instrumentcode"), lit("|"),col("location"),lit("|"),
        col("custodianaccountid"), lit("|"),col("sourcesystemid"), lit("|"),col("positiontypeid"))))

        logger.info(f"Number of rows in target_df: {target_df.count()}")

        # Perform the SCD Type 2 update (Expire old records)
        final_df = final_df.alias("source")

        columns_to_compare = [col for col in df.columns if col not in ["balance_uid", "effective_start_date",
        "effective_end_date", "current_flag"]]
        conditions = [f"target.{col} <> source.{col}" for col in columns_to_compare]
        final_condition = " OR ".join(conditions)

        updated_df = target_df_active.join(final_df, 
        on = ["balance_uid"],how = "inner").filter(final_condition)
        updated_df = updated_df.select("target.*")
        logger.info(f"Number of rows in updated_df: {updated_df.count()}")
        updated_df = updated_df.withColumn("effective_end_date", current_timestamp()) \
        .withColumn("current_flag", lit("N"))
        
        updated_df = updated_df.withColumn("balance_uid", md5(concat(col("effectivedate"), lit("|"),col("subaccountid"), lit("|"),col("instrumentcode"), lit("|"),
        col("location"), lit("|"),col("custodianaccountid"), lit("|"),col("sourcesystemid"), lit("|"),col("positiontypeid"))))
        logger.info(f"Schema of updated_df:\n {updated_df.count()}")
        logger.info(f"Schema of updated_df:\n {updated_df.show(10)}")

        # Insert new (updated) records
        new_df = final_df.join(target_df_active,
        on=["balance_uid"],how = "left_anti") \
        .withColumn("effective_start_date", cast.current_timestamp().cast(TimestampType()))) \
        .withColumn("effective_end_date", lit("Null").cast(TimestampType())) \
        .withColumn("current_flag", lit("Y").cast(StringType())) \
        new_df = new_df.select("loadflag", "effectivedate", "subaccountid", "instrumentcode", "location",
        "custodianaccountid", "quantity", "subaccountcurrencyvalue", "headaccountcurrencyvalue",
        "valuationbookcost", "pricecurrencyisocode", "pricecurrencycleanvalue",
        "subaccountcurrencyisocode", "sourcesystemid", "positiontypeid",
        "current_flag","effective_start_date","effective_end_date","balance_uid")
        logger.info(f"Number of rows in new_df: {new_df.count()}")
        logger.info(f"Schema of new_df : {new_df.show(10)}")

        # Merge the updated and new records
        final_updated_df = updated_df.union(new_df)
        logger.info(f"Number of rows in final_updated_df after union: {final_updated_df.count()}")
        logger.info(f"Show of final df last union: {final_updated_df.show(10)}")

        # Save the merged data back to the table
        final_updated_df.write.format("iceberg").mode("append").saveAsTable(f"{CATALOG_NAME}.{DB_NAME}.{tb_nm}")
        logger.info("SCD Type 2 merge completed successfully.")
except Exception as e:
    logger.error("Error during Iceberg conversion with SCD Type 2 logic: %s", e)
    raise
job.commit()


