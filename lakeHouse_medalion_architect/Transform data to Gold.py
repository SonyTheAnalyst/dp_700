#!/usr/bin/env python
# coding: utf-8

# ================================================================================
# MEDALLION ARCHITECTURE: GOLD LAYER TRANSFORMATION
# ================================================================================
# PURPOSE: Transform cleaned Silver layer data into a Star Schema with:
#          - Dimension Tables (dimdate, dimcustomer, dimproduct)
#          - Fact Table (factsales)
# 
# MEDALLION ARCHITECTURE LAYERS:
#   Bronze:  Raw data as-is from source
#   Silver:  Cleaned, deduplicated, validated data
#   Gold:    Business-ready dimensional model (Star Schema)
# ================================================================================

# ================================================================================
# IMPORTS - Required Libraries
# ================================================================================
from pyspark.sql.types import *  # Data types (DateType, StringType, etc.)
from pyspark.sql.functions import (
    col,                           # Reference DataFrame columns
    dayofmonth, month, year,       # Extract date components
    date_format,                   # Format dates (e.g., "MMM-yyyy")
    split,                         # Split strings by delimiter
    monotonically_increasing_id,   # Generate unique sequential IDs
    coalesce,                      # Return first non-null value
    max,                           # Find maximum value
    lit,                           # Create literal (constant) values
    when                           # Conditional logic (SQL CASE WHEN)
)
from delta.tables import DeltaTable  # Delta Lake table operations

# ================================================================================
# PHASE 1: DATA INGESTION FROM SILVER LAYER
# ================================================================================
# Load the cleaned, deduplicated data from Silver layer
# This data has already been validated and transformed
df = spark.read.table("sales_silver")





# ================================================================================
# PHASE 2: CREATE DIMENSION TABLE - DATE (dimdate_gold)
# ================================================================================
# A Date Dimension table enables time-based analysis (day, month, year)
# This is a "Slowly Changing Dimension" (SCD Type 0) - values don't change

# Step A: Create the Delta table structure (if it doesn't exist)
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \         # Original date value
    .addColumn("Day", IntegerType()) \            # Day of month (1-31)
    .addColumn("Month", IntegerType()) \          # Month (1-12)
    .addColumn("Year", IntegerType()) \           # Year (e.g., 2023)
    .addColumn("mmmyyyy", StringType()) \         # Format: "Jan-2023"
    .addColumn("yyyymm", StringType()) \          # Format: "202301"
    .execute()

# Step B: Transform Silver data into Date Dimension format
dfdimDate_gold = (
    df.dropDuplicates(["OrderDate"])              # Keep only unique dates
      .select(
          col("OrderDate"),                       # Original date
          dayofmonth("OrderDate").alias("Day"),   # Extract day component
          month("OrderDate").alias("Month"),      # Extract month component
          year("OrderDate").alias("Year"),        # Extract year component
          date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"),  # "Jan-2023"
          date_format(col("OrderDate"), "yyyyMM").alias("yyyymm")      # "202301"
      )
      .orderBy("OrderDate")                       # Sort chronologically
)

# Step C: UPSERT (Merge) into the Date Table
# UPSERT = UPDATE existing records + INSERT new records
dt_date = DeltaTable.forName(spark, "dbo.dimdate_gold")
dt_date.alias("gold") \
    .merge(
        dfdimDate_gold.alias("updates"),
        "gold.OrderDate = updates.OrderDate"      # Match key: OrderDate
    ) \
    .whenNotMatchedInsertAll() \                  # Insert if new date doesn't exist
    .execute()




# ================================================================================
# PHASE 3: CREATE DIMENSION TABLE - CUSTOMER (dimcustomer_gold)
# ================================================================================
# A Customer Dimension provides a single "version of truth" for customer data
# Uses a SURROGATE KEY (CustomerID) instead of natural key (CustomerName, Email)
# This allows for dimensional changes without breaking fact table relationships

# Step A: Create the Customer Delta table structure
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \    # Full customer name
    .addColumn("Email", StringType()) \           # Email address
    .addColumn("First", StringType()) \           # First name (split from full name)
    .addColumn("Last", StringType()) \            # Last name (split from full name)
    .addColumn("CustomerID", LongType()) \        # SURROGATE KEY (unique identifier)
    .execute()

# Step B: Filter for NEW customers only
# Left Anti Join: Returns records from LEFT table that have NO match in RIGHT table
df_existing_cust = spark.read.table("dbo.dimcustomer_gold")  # Get existing customers

df_new_customers = (
    df.dropDuplicates(["CustomerName", "Email"])             # Remove duplicate customers
      .select("CustomerName", "Email")                       # Select relevant columns
      .withColumn("First", split(col("CustomerName"), " ").getItem(0))  # First word = First name
      .withColumn("Last", split(col("CustomerName"), " ").getItem(1))   # Second word = Last name
      .join(df_existing_cust, ["CustomerName", "Email"], "leftanti")    # Keep only NEW customers
)

# Step C: Generate Incremental IDs (Surrogate Keys)
# Ensures CustomerID values are continuous and don't have gaps
max_cust_id = df_existing_cust.select(
    coalesce(max(col("CustomerID")), lit(0))    # Get max ID, or 0 if table is empty
).first()[0]

df_cust_to_insert = df_new_customers.withColumn(
    "CustomerID",
    monotonically_increasing_id() + max_cust_id + 1  # New IDs start after max existing ID
)

# Step D: Merge new customer records
dt_cust = DeltaTable.forName(spark, "dbo.dimcustomer_gold")
dt_cust.alias("gold") \
    .merge(
        df_cust_to_insert.alias("updates"),
        "gold.Email = updates.Email"              # Match key: Email is unique identifier
    ) \
    .whenNotMatchedInsertAll() \                  # Insert if new customer
    .execute()





# ================================================================================
# PHASE 4: CREATE DIMENSION TABLE - PRODUCT (dimproduct_gold)
# ================================================================================
# A Product Dimension provides a single source for product information
# Uses surrogate key (ItemID) for efficient relationships

# Step A: Create the Product Delta table structure
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \       # Product name (first part of "Item" field)
    .addColumn("ItemID", LongType()) \           # SURROGATE KEY
    .addColumn("ItemInfo", StringType()) \       # Product metadata/description
    .execute()

# Step B: Extract unique products and split their names
df_existing_prod = spark.read.table("dbo.dimproduct_gold")  # Get existing products

df_prod_silver = (
    df.dropDuplicates(["Item"]).select(col("Item"))          # Get unique product names
      .withColumn("ItemName", split(col("Item"), " ").getItem(0))  # First word = product name
      .withColumn(
          "ItemInfo",
          # If second word is null or empty, set ItemInfo to empty string
          # Otherwise, use the second word as metadata
          when(
              (split(col("Item"), " ").getItem(1).isNull()) |  # OR operator
              (split(col("Item"), " ").getItem(1) == ""),
              lit("")
          ).otherwise(split(col("Item"), " ").getItem(1))
      )
      .join(df_existing_prod, ["ItemName", "ItemInfo"], "leftanti")  # Keep only NEW products
)

# Step C: Generate Incremental Product IDs
max_prod_id = df_existing_prod.select(
    coalesce(max(col("ItemID")), lit(0))        # Get max ID, or 0 if table is empty
).first()[0]

df_prod_to_insert = df_prod_silver.withColumn(
    "ItemID",
    monotonically_increasing_id() + max_prod_id + 1  # New IDs start after max existing ID
)

# Step D: Merge new product records
dt_prod = DeltaTable.forName(spark, "dbo.dimproduct_gold")
dt_prod.alias("gold") \
    .merge(
        df_prod_to_insert.alias("updates"),
        # Match key: Combination of ItemName AND ItemInfo uniquely identifies a product
        "gold.ItemName = updates.ItemName AND gold.ItemInfo = updates.ItemInfo"
    ) \
    .whenNotMatchedInsertAll() \                 # Insert if new product
    .execute()






# ================================================================================
# PHASE 5: CREATE FACT TABLE - SALES (factsales_gold)
# ================================================================================
# The Fact Table contains the measurable events (sales transactions)
# Uses FOREIGN KEYS (surrogate keys from dimensions) instead of business keys
# This creates a STAR SCHEMA for analytics:
#
#              dimdate_gold
#                    |
#        dimcustomer_gold --|-- factsales_gold --|-- dimproduct_gold
#
# Benefits of Star Schema:
#   - Fast joins on dimension tables
#   - Easy to understand for business users
#   - Optimized for analytical queries (OLAP)

# Step A: Create the Fact table structure
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.factsales_gold") \
    .addColumn("CustomerID", LongType()) \      # FOREIGN KEY → dimcustomer_gold
    .addColumn("ItemID", LongType()) \          # FOREIGN KEY → dimproduct_gold
    .addColumn("OrderDate", DateType()) \       # FOREIGN KEY → dimdate_gold
    .addColumn("Quantity", IntegerType()) \     # Measure (sales quantity)
    .addColumn("UnitPrice", FloatType()) \      # Measure (price per unit)
    .addColumn("Tax", FloatType()) \            # Measure (tax amount)
    .execute()

# Step B: Prepare Silver data for joining with dimensions
# First, apply the same transformations as Product dimension so joins work correctly
df_fact_prep = df.withColumn("ItemName", split(col("Item"), " ").getItem(0)) \
                 .withColumn(
                     "ItemInfo",
                     when(
                         (split(col("Item"), " ").getItem(1).isNull()) |
                         (split(col("Item"), " ").getItem(1) == ""),
                         lit("")
                     ).otherwise(split(col("Item"), " ").getItem(1))
                 )

# Re-read Gold Dimensions for fresh, latest ID Lookups
dimCust = spark.read.table("dbo.dimcustomer_gold")
dimProd = spark.read.table("dbo.dimproduct_gold")

# Step C: Build Fact Table by joining with Dimensions
# This replaces customer/product names with their IDs (denormalization for performance)
dfFactSales_gold = df_fact_prep.alias("f") \
    .join(dimCust.alias("c"), ["CustomerName", "Email"], "left") \  # Get CustomerID
    .join(dimProd.alias("p"), ["ItemName", "ItemInfo"], "left") \    # Get ItemID
    .select(
        col("c.CustomerID"),        # From Customer dimension
        col("p.ItemID"),            # From Product dimension
        col("f.OrderDate"),         # From fact/silver data
        col("f.Quantity"),          # From fact/silver data
        col("f.UnitPrice"),         # From fact/silver data
        col("f.Tax")                # From fact/silver data
    ).orderBy("OrderDate")          # Sort by date for easier analysis

# Step D: Final UPSERT into Fact Table
dt_fact = DeltaTable.forName(spark, "dbo.factsales_gold")
dt_fact.alias("gold") \
    .merge(
        dfFactSales_gold.alias("updates"),
        # Match key: Composite key ensures uniqueness of sales transactions
        # Same customer + same item + same date = same transaction (no duplicates)
        "gold.OrderDate = updates.OrderDate AND " +
        "gold.CustomerID = updates.CustomerID AND " +
        "gold.ItemID = updates.ItemID"
    ) \
    .whenNotMatchedInsertAll() \    # Insert if new sale doesn't exist
    .execute()

# Success message
print("Star Schema (Gold) successfully populated.")
