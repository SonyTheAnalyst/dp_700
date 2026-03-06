"""
Pipeline: Sales Data - Bronze to Silver
Description: This script ingests raw CSV sales data, cleans/enriches it, 
             and performs an incremental UPSERT into a Delta table.
Architecture: Medallion (Bronze -> Silver)
"""

from pyspark.sql.types import *
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
from delta.tables import *

# ======================================================================================
# 1. SCHEMA DEFINITION & INGESTION (BRONZE LAYER)
# Enforces "Schema-on-Read" to ensure data integrity and correct typing.
# ======================================================================================

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

# Load raw CSVs from Bronze using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(orderSchema) \
    .load("Files/bronze/*.csv")

# ======================================================================================
# 2. CLEANING & ENRICHMENT (SILVER TRANSFORMATION)
# Sanitize the data and add technical metadata for auditing and lineage.
# ======================================================================================

df = df.withColumn("FileName", input_file_name()) \
    .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01', True).otherwise(False)) \
    .withColumn("CreatedTS", current_timestamp()) \
    .withColumn("ModifiedTS", current_timestamp())

# Data Quality: Replace empty or NULL names with "Unknown"
df = df.withColumn(
    "CustomerName",
    when((col("CustomerName").isNull()) | (col("CustomerName") == ""), lit("Unknown"))
    .otherwise(col("CustomerName"))
)

# ======================================================================================
# 3. TARGET DELTA TABLE INITIALIZATION
# Ensures the Silver table exists in the metastore before the merge operation.
# ======================================================================================

DeltaTable.createIfNotExists(spark) \
    .tableName("sales.dbo.sales_silver") \
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("SalesOrderLineNumber", IntegerType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("Item", StringType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .addColumn("FileName", StringType()) \
    .addColumn("IsFlagged", BooleanType()) \
    .addColumn("CreatedTS", TimestampType()) \
    .addColumn("ModifiedTS", TimestampType()) \
    .execute()

# ======================================================================================
# 4. INCREMENTAL LOADING (MERGE / UPSERT)
# Synchronizes the DataFrame with the Delta table to prevent duplicate records.
# ======================================================================================

# Reference the existing physical Delta table
target_path = "abfss://dp700lab6@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/dbo/sales_silver"
deltaTable = DeltaTable.forPath(spark, target_path)

# Execute MERGE logic based on unique business keys
deltaTable.alias("silver") \
    .merge(
        df.alias("updates"),
        """
        silver.SalesOrderNumber = updates.SalesOrderNumber AND
        silver.OrderDate = updates.OrderDate AND
        silver.CustomerName = updates.CustomerName AND
        silver.Item = updates.Item
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

##WE CAN USE THE FOLLOWING CODE BELLOW TO INSERT PARTICULAR FIELDS
.whenMatchedUpdate(set={
    .whenMatchedUpdateAll() \
        # Existing records could be updated here (e.g., if UnitPrice changed)
    .whenNotMatchedInsertAll() \
    }) \
    .execute()
    .whenNotMatchedInsert(values={
        # If the record doesn't exist in Silver, insert the new data
        "SalesOrderNumber": "updates.SalesOrderNumber",
        "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
        "OrderDate": "updates.OrderDate",
        "CustomerName": "updates.CustomerName",
        "Email": "updates.Email",
        "Item": "updates.Item",
        "Quantity": "updates.Quantity",
        "UnitPrice": "updates.UnitPrice",
        "Tax": "updates.Tax",
        "FileName": "updates.FileName",
        "IsFlagged": "updates.IsFlagged",
        "CreatedTS": "updates.CreatedTS",
        "ModifiedTS": "updates.ModifiedTS"
    }) \
    .execute()
