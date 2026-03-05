#!/usr/bin/env python
# coding: utf-8

# ## Transform data to Gold
# 
# New notebook

# In[36]:


df = spark.read.table("sales_silver")
display(df)


# ### _**CREATE A DELTA GOLD TABLE**_

# In[2]:


from pyspark.sql.types import *
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()


# In[3]:


from pyspark.sql.functions import col, dayofmonth, month, year, date_format

# Create the dimDate_gold dataframe from distinct OrderDate values
dfdimDate_gold = (
    df.dropDuplicates(["OrderDate"])                # keep one row per date
      .select(
          col("OrderDate"),
          dayofmonth("OrderDate").alias("Day"),     # extract day number
          month("OrderDate").alias("Month"),        # extract month number
          year("OrderDate").alias("Year"),          # extract year number
          date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"),  # e.g. Jan-2021
          date_format(col("OrderDate"), "yyyyMM").alias("yyyymm")      # e.g. 202101
      )
      .orderBy("OrderDate")                         # sort chronologically
)

# Preview the first 10 rows
display(dfdimDate_gold)


# In[5]:


from delta.tables import *

# Load the Delta table from the metastore
deltaTable = DeltaTable.forPath(spark, "abfss://dp700lab6@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/dbo/dimdate_gold")

# Your dimdate DataFrame
dfUpdates = dfdimDate_gold

# Perform MERGE: update nothing, insert new rows
deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.OrderDate = updates.OrderDate"
    ) \
    .whenMatchedUpdate(set={
        # No updates in your screenshot — leave empty
    }) \
    .whenNotMatchedInsert(values={
        "OrderDate": "updates.OrderDate",
        "Day": "updates.Day",
        "Month": "updates.Month",
        "Year": "updates.Year",
        "mmmyyyy": "updates.mmmyyyy",
        "yyyymm": "updates.yyyymm"
    }) \
    .execute()


# In[6]:


from pyspark.sql.types import *
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("First", StringType()) \
    .addColumn("Last", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()


# ### **This transformation prepares a clean customer dimension by extracting unique customers from your Silver fact table and splitting their names into first and last components**
# 

# #### **Dimension table customer**

# In[7]:


from pyspark.sql.functions import col, split

# Create the dimCustomer_silver dataframe
dfdimCustomer_silver = (
    df.dropDuplicates(["CustomerName", "Email"])     # keep one row per customer
      .select(
          col("CustomerName"),
          col("Email")
      )
      .withColumn("First", split(col("CustomerName"), " ").getItem(0))  # first name
      .withColumn("Last", split(col("CustomerName"), " ").getItem(1))   # last name
)

# Preview the first 10 rows
display(dfdimCustomer_silver)


# In[12]:


from pyspark.sql.functions import monotonically_increasing_id, col, when, coalesce, max, lit
#- ID generation (monotonically_increasing_id)
#-- null handling (coalesce)

dfdimCustomer_temp = spark.read.table("dbo.dimCustomer_gold")
display(dfdimCustomer_temp)

MAXCustomerID = dfdimCustomer_temp.select(
    coalesce(max(col("CustomerID")), lit(0)).alias("MAXCustomerID")
).first()[0]

dfdimCustomer_gold = dfdimCustomer_silver.join(
    dfdimCustomer_temp,
    (dfdimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName) &
    (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email),
    "leftanti"
)

dfdimCustomer_gold = dfdimCustomer_gold.withColumn(
    "CustomerID",
    monotonically_increasing_id() + MAXCustomerID + 1
)

display(dfdimCustomer_gold)


# In[13]:


from delta.tables import *

# Load the Delta table from the metastore
deltaTable = DeltaTable.forName(spark, "dbo.dimcustomer_gold")

# New customer rows that need to be inserted
dfUpdates = dfdimCustomer_gold

# Perform MERGE: update nothing, insert new customers
deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.CustomerName = updates.CustomerName AND gold.Email = updates.Email"
    ) \
    .whenMatchedUpdate(set={
        # No updates needed (same as your screenshot)
    }) \
    .whenNotMatchedInsert(values={
        "CustomerName": "updates.CustomerName",
        "Email": "updates.Email",
        "First": "updates.First",
        "Last": "updates.Last",
        "CustomerID": "updates.CustomerID"
    }) \
    .execute()


# ### **A Product Dimension is part of your Gold layer. It provides:**
# ### - **A unique list of products (ItemName)**
# ### - **A surrogate key (ItemID)**
# ### - **Optional product metadata (ItemInfo)**
# ### **This dimension is used to join with your fact table (sales_silver or sales_gold) in a star schema.**
# 
# 

# In[14]:


DeltaTable.createIfNotExists(spark) \
  .tableName("dbo.dimproduct_gold") \
  .addColumn("ItemName", StringType()) \
  .addColumn("ItemID", LongType()) \
  .addColumn("ItemInfo", StringType()) \
  .execute()


# In[41]:


from pyspark.sql.functions import col, split, lit, when

# Create product_silver dataframe
dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName", split(col("Item"), " ").getItem(0)) \
    .withColumn(
        "ItemInfo",
         when(
            (split(
                col("Item"), " "
                ).getItem(1).isNull()
            ) | (
                split(col("Item"), " ").getItem(1)==""
                ), lit("")).otherwise(
                    split(col("Item"), " ").getItem(1)
                    )
                    )

# Display the first 10 rows of the dataframe to preview your data
display(dfdimProduct_silver)


# In[18]:


from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce

# Load existing Gold product dimension
dfdimProduct_temp = spark.read.table("dbo.dimProduct_gold")

# Find the highest existing ItemID (or 0 if table is empty)
MAXProductID = dfdimProduct_temp.select(
    coalesce(max(col("ItemID")), lit(0)).alias("MAXItemID")
).first()[0]

# Identify NEW products (those not already in dimProduct_gold)
dfdimProduct_gold = dfdimProduct_silver.join(
    dfdimProduct_temp,
    (dfdimProduct_silver.ItemName == dfdimProduct_temp.ItemName) &
    (dfdimProduct_silver.ItemInfo == dfdimProduct_temp.ItemInfo),
    "leftanti"
)

# Assign new surrogate ItemIDs
dfdimProduct_gold = dfdimProduct_gold.withColumn(
    "ItemID",
    monotonically_increasing_id() + MAXProductID + 1
)

# Preview
display(dfdimProduct_gold)


# In[29]:


from delta.tables import *

# Load the Delta table from the metastore
deltaTable = DeltaTable.forPath(spark, "abfss://dp700lab6@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/dbo/dimproduct_gold")

# New product rows that need to be inserted
dfUpdates = dfdimProduct_gold

# Perform MERGE: update nothing, insert new products
deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.ItemName = updates.ItemName AND gold.ItemInfo = updates.ItemInfo"
    ) \
    .whenMatchedUpdate(set={
        # No updates needed (same as your screenshot)
    }) \
    .whenNotMatchedInsert(values={
        "ItemName": "updates.ItemName",
        "ItemInfo": "updates.ItemInfo",
        "ItemID": "updates.ItemID"
    }) \
    .execute()


# In[ ]:





# ## **FACT SALES GOLD TABLE**

# In[25]:


from pyspark.sql.types import *
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()


# In[37]:


display (df)


# In[39]:


from pyspark.sql.functions import col

dfdimCustomer_temp = spark.read.table("dbo.dimCustomer_gold")
dfdimProduct_temp = spark.read.table("dbo.dimProduct_gold")

df = df.withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1)))

# Create Sales_gold dataframe
dfFactSales_gold = df.alias("df1").join(dfdimCustomer_temp.alias("df2"), 
                                        (df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email), "left") \
                        .join(dfdimProduct_temp.alias("df3"),(df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemID), "left") \
    .select(col("df2.CustomerID") \
        , col("df3.ItemID") \
        , col("df1.OrderDate") \
        , col("df1.Quantity") \
        , col("df1.UnitPrice") \
        , col("df1.Tax")) \
    .orderBy(col("df1.OrderDate"), col("df2.CustomerID"), col("df3.ItemID"))


# Display the first 10 rows of the dataframe to preview your data
display(dfFactSales_gold)
display(df)


# In[42]:


from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "abfss://dp700lab6@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/dbo/factsales_gold")

dfUpdates = dfFactSales_gold

deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.OrderDate = updates.OrderDate AND gold.CustomerID = updates.CustomerID AND gold.ItemID = updates.ItemID'
    ) \
    .whenMatchedUpdate(set =
        {
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "CustomerID": "updates.CustomerID",
            "ItemID": "updates.ItemID",
            "OrderDate": "updates.OrderDate",
            "Quantity": "updates.Quantity",
            "UnitPrice": "updates.UnitPrice",
            "Tax": "updates.Tax"
        }
    ) \
    .execute()

