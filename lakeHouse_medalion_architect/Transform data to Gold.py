#!/usr/bin/env python
# coding: utf-8

# ## Transform data to Gold
# 
# New notebook

# In[36]:


from pyspark.sql.types import *
from pyspark.sql.functions import col, dayofmonth, month, year, date_format, split, monotonically_increasing_id
from delta.tables import *

# ==========================================
# 1. DATA INGESTION FROM SILVER
# ==========================================
# Loading the cleaned data processed in the previous stage
df = spark.read.table("sales_silver")

# ==========================================
# 2. CREATE DIMENSION: DATE (dimdate_gold)
# ==========================================

# Step A: Create physical Delta table if it doesn't exist
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

# Step B: Transform Silver data into Date Dimension format
dfdimDate_gold = (
    df.dropDuplicates(["OrderDate"])
      .select(
          col("OrderDate"),
          dayofmonth("OrderDate").alias("Day"),
          month("OrderDate").alias("Month"),
          year("OrderDate").alias("Year"),
          date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"),
          date_format(col("OrderDate"), "yyyyMM").alias("yyyymm")
      )
      .orderBy("OrderDate")
)

# Step C: UPSERT (Merge) into the Date Table
dt_date = DeltaTable.forName(spark, "dbo.dimdate_gold")
dt_date.alias("gold") \
    .merge(dfdimDate_gold.alias("updates"), "gold.OrderDate = updates.OrderDate") \
    .whenNotMatchedInsertAll() \
    .execute()

# ==========================================
# 3. CREATE DIMENSION: CUSTOMER (dimcustomer_gold)
# ==========================================

# Step A: Create the Customer Delta table
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()

# Step B: Prepare Customer DataFrame with Surrogate Keys (IDs)
dfdimCustomer_gold = df.dropDuplicates(["CustomerName", "Email"]) \
    .select("CustomerName", "Email") \
    .withColumn("CustomerID", monotonically_increasing_id())

# Step C: UPSERT into Customer Table
dt_cust = DeltaTable.forName(spark, "dbo.dimcustomer_gold")
dt_cust.alias("gold") \
    .merge(dfdimCustomer_gold.alias("updates"), "gold.Email = updates.Email") \
    .whenNotMatchedInsertAll() \
    .execute()

# ==========================================
# 4. CREATE FACT TABLE: SALES (factsales_gold)
# ==========================================

# Step A: Create the Fact table structure
# Note: We use IDs here instead of names for performance
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

# Step B: Join Silver data with Gold Dimensions to get IDs
dfFactSales_gold = df.alias("s") \
    .join(dfdimCustomer_gold.alias("c"), "Email", "left") \
    .select(
        col("c.CustomerID"),
        col("s.OrderDate"),
        col("s.Quantity"),
        col("s.UnitPrice"),
        col("s.Tax")
    )

# Step C: Final UPSERT into Fact Table
dt_fact = DeltaTable.forName(spark, "dbo.factsales_gold")
dt_fact.alias("gold") \
    .merge(
        dfFactSales_gold.alias("updates"), 
        "gold.OrderDate = updates.OrderDate AND gold.CustomerID = updates.CustomerID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

print("Gold Layer Transformation Complete.")

# ### **This transformation prepares a clean customer dimension by extracting unique customers from your Silver fact table and splitting their names into first and last components**
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, dayofmonth, month, year, date_format, 
    split, monotonically_increasing_id, coalesce, max, lit
)
from delta.tables import *

# ==========================================
# 1. DATA INGESTION
# ==========================================
# Load cleaned Silver data
df = spark.read.table("sales_silver")

# ==========================================
# 2. DIMENSION: DATE (dimdate_gold)
# ==========================================

# Step A: Ensure table exists
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

# Step B: Transform
dfdimDate_gold = (
    df.dropDuplicates(["OrderDate"])
      .select(
          col("OrderDate"),
          dayofmonth("OrderDate").alias("Day"),
          month("OrderDate").alias("Month"),
          year("OrderDate").alias("Year"),
          date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"),
          date_format(col("OrderDate"), "yyyyMM").alias("yyyymm")
      )
)

# Step C: Merge
dt_date = DeltaTable.forName(spark, "dbo.dimdate_gold")
dt_date.alias("gold") \
    .merge(dfdimDate_gold.alias("updates"), "gold.OrderDate = updates.OrderDate") \
    .whenNotMatchedInsertAll() \
    .execute()

# ==========================================
# 3. DIMENSION: CUSTOMER (dimcustomer_gold)
# ==========================================

# Step A: Ensure table exists
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("First", StringType()) \
    .addColumn("Last", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()

# Step B: Filter for NEW customers only (Left Anti Join)
df_existing_cust = spark.read.table("dbo.dimcustomer_gold")

df_new_customers = (
    df.dropDuplicates(["CustomerName", "Email"])
      .select("CustomerName", "Email")
      .withColumn("First", split(col("CustomerName"), " ").getItem(0))
      .withColumn("Last", split(col("CustomerName"), " ").getItem(1))
      .join(df_existing_cust, ["CustomerName", "Email"], "leftanti")
)

# Step C: Generate Incremental IDs
# Get current max ID or 0 if table is empty
max_id_row = df_existing_cust.select(coalesce(max(col("CustomerID")), lit(0))).first()
max_id = max_id_row[0] if max_id_row else 0

df_customers_to_insert = df_new_customers.withColumn(
    "CustomerID", 
    monotonically_increasing_id() + max_id + 1
)

# Step D: Merge new records
dt_cust = DeltaTable.forName(spark, "dbo.dimcustomer_gold")
dt_cust.alias("gold") \
    .merge(
        df_customers_to_insert.alias("updates"), 
        "gold.CustomerName = updates.CustomerName AND gold.Email = updates.Email"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# ==========================================
# 4. FACT TABLE: SALES (factsales_gold)
# ==========================================

# Step A: Ensure table exists
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

# Step B: Join with Gold Customers to get the correct Surrogate Keys
df_final_cust_lookup = spark.read.table("dbo.dimcustomer_gold")

df_fact_sales = df.alias("s") \
    .join(df_final_cust_lookup.alias("c"), ["CustomerName", "Email"], "left") \
    .select(
        col("c.CustomerID"),
        col("s.OrderDate"),
        col("s.Quantity"),
        col("s.UnitPrice"),
        col("s.Tax")
    )

# Step C: Final Merge
dt_fact = DeltaTable.forName(spark, "dbo.factsales_gold")
dt_fact.alias("gold") \
    .merge(
        df_fact_sales.alias("updates"),
        "gold.OrderDate = updates.OrderDate AND gold.CustomerID = updates.CustomerID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

print("Gold Star Schema successfully updated.")


# ### **A Product Dimension is part of your Gold layer. It provides:**
# ### - **A unique list of products (ItemName)**
# ### - **A surrogate key (ItemID)**
# ### - **Optional product metadata (ItemInfo)**
# ### **This dimension is used to join with your fact table (sales_silver or sales_gold) in a star schema.**
# 
# 

# In[14]:


from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, dayofmonth, month, year, date_format, 
    split, monotonically_increasing_id, coalesce, max, lit, when
)
from delta.tables import *

# ==============================================================================
# 1. DATA INGESTION
# ==============================================================================
df = spark.read.table("sales_silver")

# ==============================================================================
# 2. DIMENSION: DATE (dimdate_gold)
# ==============================================================================
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

dfdimDate_gold = (
    df.dropDuplicates(["OrderDate"])
      .select(
          col("OrderDate"),
          dayofmonth("OrderDate").alias("Day"),
          month("OrderDate").alias("Month"),
          year("OrderDate").alias("Year"),
          date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"),
          date_format(col("OrderDate"), "yyyyMM").alias("yyyymm")
      )
)

dt_date = DeltaTable.forName(spark, "dbo.dimdate_gold")
dt_date.alias("gold") \
    .merge(dfdimDate_gold.alias("updates"), "gold.OrderDate = updates.OrderDate") \
    .whenNotMatchedInsertAll() \
    .execute()

# ==============================================================================
# 3. DIMENSION: CUSTOMER (dimcustomer_gold)
# ==============================================================================
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("First", StringType()) \
    .addColumn("Last", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()

df_existing_cust = spark.read.table("dbo.dimcustomer_gold")

# Process new customers with Name Splitting
df_new_customers = (
    df.dropDuplicates(["CustomerName", "Email"])
      .select("CustomerName", "Email")
      .withColumn("First", split(col("CustomerName"), " ").getItem(0))
      .withColumn("Last", split(col("CustomerName"), " ").getItem(1))
      .join(df_existing_cust, ["CustomerName", "Email"], "leftanti")
)

# Incremental ID Generation
max_cust_id = df_existing_cust.select(coalesce(max(col("CustomerID")), lit(0))).first()[0]
df_cust_to_insert = df_new_customers.withColumn("CustomerID", monotonically_increasing_id() + max_cust_id + 1)

dt_cust = DeltaTable.forName(spark, "dbo.dimcustomer_gold")
dt_cust.alias("gold") \
    .merge(df_cust_to_insert.alias("updates"), "gold.Email = updates.Email") \
    .whenNotMatchedInsertAll() \
    .execute()

# ==============================================================================
# 4. DIMENSION: PRODUCT (dimproduct_gold)
# ==============================================================================
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("ItemInfo", StringType()) \
    .execute()

df_existing_prod = spark.read.table("dbo.dimproduct_gold")

# Extract unique items and split metadata
df_prod_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName", split(col("Item"), " ").getItem(0)) \
    .withColumn("ItemInfo", 
                when((split(col("Item"), " ").getItem(1).isNull()) | 
                     (split(col("Item"), " ").getItem(1) == ""), lit(""))
                .otherwise(split(col("Item"), " ").getItem(1))) \
    .join(df_existing_prod, ["ItemName", "ItemInfo"], "leftanti")

# Incremental ID Generation
max_prod_id = df_existing_prod.select(coalesce(max(col("ItemID")), lit(0))).first()[0]
df_prod_to_insert = df_prod_silver.withColumn("ItemID", monotonically_increasing_id() + max_prod_id + 1)

dt_prod = DeltaTable.forName(spark, "dbo.dimproduct_gold")
dt_prod.alias("gold") \
    .merge(df_prod_to_insert.alias("updates"), "gold.ItemName = updates.ItemName AND gold.ItemInfo = updates.ItemInfo") \
    .whenNotMatchedInsertAll() \
    .execute()

# ==============================================================================
# 5. FACT TABLE: SALES (factsales_gold)
# ==============================================================================
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

# Re-read Gold Dimensions for fresh ID Lookups
dimCust = spark.read.table("dbo.dimcustomer_gold")
dimProd = spark.read.table("dbo.dimproduct_gold")

# Prepare Silver Data for joining (matching the Product split logic)
df_fact_prep = df.withColumn("ItemName", split(col("Item"), " ").getItem(0)) \
                 .withColumn("ItemInfo", 
                             when((split(col("Item"), " ").getItem(1).isNull()) | 
                                  (split(col("Item"), " ").getItem(1) == ""), lit(""))
                             .otherwise(split(col("Item"), " ").getItem(1)))

# Build Fact Table by joining with Dimensions
dfFactSales_gold = df_fact_prep.alias("f") \
    .join(dimCust.alias("c"), ["CustomerName", "Email"], "left") \
    .join(dimProd.alias("p"), ["ItemName", "ItemInfo"], "left") \
    .select(
        col("c.CustomerID"),
        col("p.ItemID"),
        col("f.OrderDate"),
        col("f.Quantity"),
        col("f.UnitPrice"),
        col("f.Tax")
    ).orderBy("OrderDate")

# Final Merge into Fact Table
dt_fact = DeltaTable.forName(spark, "dbo.factsales_gold")
dt_fact.alias("gold") \
    .merge(dfFactSales_gold.alias("updates"), 
           "gold.OrderDate = updates.OrderDate AND gold.CustomerID = updates.CustomerID AND gold.ItemID = updates.ItemID") \
    .whenNotMatchedInsertAll() \
    .execute()

print("Star Schema (Gold) successfully populated.")

