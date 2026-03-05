#1. Schema Definition & Ingestion (Bronze Layer)
#This stage defines the expected structure of the raw CSV files to ensure data integrity during the read process.

from pyspark.sql.types import *

# Define the structure (Schema) for the sales data
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

# Read all CSV files from the 'bronze' folder
# .schema(orderSchema) forces column types (e.g., converting text to Date or Float)
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(orderSchema) \
    .load("Files/bronze/*.csv")

display(df)
#======================================================================================================================
#======================================================================================================================


#2. Cleaning & Enrichment (Silver Transformation)
#In this step, we add technical metadata for auditing and handle missing values to "sanitize" the data.

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name

# Add technical metadata columns for auditing
# FileName: captures the source file path for data lineage
# IsFlagged: business logic to mark orders older than August 2019
# CreatedTS/ModifiedTS: record when the row entered the Silver layer
df = df.withColumn("FileName", input_file_name()) \
    .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01', True).otherwise(False)) \
    .withColumn("CreatedTS", current_timestamp()) \
    .withColumn("ModifiedTS", current_timestamp())

# Clean 'CustomerName' column
# If the name is null or an empty string, replace it with "Unknown"
df = df.withColumn(
    "CustomerName",
    when(
        (col("CustomerName").isNull()) | (col("CustomerName") == ""),
        lit("Unknown")
    ).otherwise(col("CustomerName"))
)

display(df)
#======================================================================================================================
#======================================================================================================================

#3. Target Delta Table Creation
#Before loading, we ensure the physical Delta table exists in the metastore with the correct schema.

from pyspark.sql.types import *
from delta.tables import *

# Create the 'sales_silver' Delta table if it doesn't already exist
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
    .addColumn("CreatedTS", DateType()) \
    .addColumn("ModifiedTS", DateType()) \
    .execute()



#4. Incremental Loading (Merge / Upsert)
#This is the most critical part: synchronizing the cleaned DataFrame (updates) with the physical table (silver) using a Merge operation to prevent duplicates.

from delta.tables import *

# Load the existing Delta table from its OneLake storage path
deltaTable = DeltaTable.forPath(spark, "abfss://dp700lab6@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/dbo/sales_silver")

# Execute MERGE logic
deltaTable.alias("silver") \
    .merge(
        df.alias("updates"),
        # Join condition: identify unique rows using these 4 keys
        """
        silver.SalesOrderNumber = updates.SalesOrderNumber AND
        silver.OrderDate = updates.OrderDate AND
        silver.CustomerName = updates.CustomerName AND
        silver.Item = updates.Item
        """
    ) \
    .whenMatchedUpdate(set={
        # Existing records could be updated here (e.g., if UnitPrice changed)
    }) \
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
















