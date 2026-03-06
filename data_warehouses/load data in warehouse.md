https://learn.microsoft.com/en-us/training/modules/load-data-into-microsoft-fabric-data-warehouse/2-explore-data-load-strategies



## Create a table in the lakehouse

1. In the **...** menu for the **sales.csv** file in the **Explorer** pane, select **Load to tables**, and then **New table**.

1. Provide the following information in the **Load file to new table** dialog.
    - **New table name:** staging_sales
    - **Use header for columns names:** Selected
    - **Separator:** ,

1. Select **Load**.

## Create a warehouse

Now that you have a workspace, a lakehouse, and the sales table with the data you need, it's time to create a data warehouse.

1. On the menu bar on the left, select **Create**. In the *New* page, under the *Data Warehouse* section, select **Warehouse**. Give it a unique name of your choice.

    >**Note**: If the **Create** option is not pinned to the sidebar, you need to select the ellipsis (**...**) option first.

    After a minute or so, a new warehouse will be created:

    ![Screenshot of a new warehouse.](./Images/new-empty-data-warehouse.png)


   ## Create fact table, dimensions and view

Let's create the fact tables and dimensions for the Sales data. You'll also create a view pointing to a lakehouse, this simplifies the code in the stored procedure we'll use to load.

1. From your workspace, select the warehouse you created.

1. In the warehouse toolbar, select **New SQL query**, then copy and run the following query.

    ```sql
    CREATE SCHEMA [Sales]
    GO
        
    IF OBJECT_ID('Sales.Fact_Sales', 'U') IS NULL
    	CREATE TABLE Sales.Fact_Sales (
    		CustomerID VARCHAR(255) NOT NULL,
    		ItemID VARCHAR(255) NOT NULL,
    		SalesOrderNumber VARCHAR(30),
    		SalesOrderLineNumber INT,
    		OrderDate DATE,
    		Quantity INT,
    		TaxAmount FLOAT,
    		UnitPrice FLOAT
    	);
    
    IF OBJECT_ID('Sales.Dim_Customer', 'U') IS NULL
        CREATE TABLE Sales.Dim_Customer (
            CustomerID VARCHAR(255) NOT NULL,
            CustomerName VARCHAR(255) NOT NULL,
            EmailAddress VARCHAR(255) NOT NULL
        );
        
    ALTER TABLE Sales.Dim_Customer add CONSTRAINT PK_Dim_Customer PRIMARY KEY NONCLUSTERED (CustomerID) NOT ENFORCED
    GO
    
    IF OBJECT_ID('Sales.Dim_Item', 'U') IS NULL
        CREATE TABLE Sales.Dim_Item (
            ItemID VARCHAR(255) NOT NULL,
            ItemName VARCHAR(255) NOT NULL
        );
        
    ALTER TABLE Sales.Dim_Item add CONSTRAINT PK_Dim_Item PRIMARY KEY NONCLUSTERED (ItemID) NOT ENFORCED
    GO
    ```

    > **Important:** In a data warehouse, foreign key constraints are not always necessary at the table level. While foreign key constraints can help ensure data integrity,
    > they can also add overhead to the ETL (Extract, Transform, Load) process and slow down data loading.
    > The decision to use foreign key constraints in a data warehouse should be based on a careful consideration of the trade-offs between data integrity and performance.

## STORED PROCEDURE

Let's create the fact tables and dimensions for the Sales data. You'll also create a view pointing to a lakehouse, this simplifies the code in the stored procedure we'll use to load.

1. From your workspace, select the warehouse you created.

1. In the warehouse toolbar, select **New SQL query**, then copy and run the following query.
 ```sql
    -- =====================================================================
-- STORED PROCEDURE: Sales.LoadDataFromStaging
-- =====================================================================
-- PURPOSE: 
--   Loads data from a staging table into dimensional and fact tables 
--   following a star schema design pattern (used in data warehousing)
--   for a specific year.
--
-- PARAMETERS:
--   @OrderYear INT - The year to filter data for loading (e.g., 2024)
--
-- PROCESS FLOW:
--   1. Load unique customers into Dim_Customer (Dimension Table)
--   2. Load unique items into Dim_Item (Dimension Table)
--   3. Load sales transactions into Fact_Sales (Fact Table)
-- =====================================================================

CREATE OR ALTER PROCEDURE Sales.LoadDataFromStaging (@OrderYear INT)
AS
BEGIN

	-- =====================================================================
	-- STEP 1: LOAD CUSTOMER DIMENSION TABLE
	-- =====================================================================
	-- PURPOSE: Extract unique customers from staging and insert them into 
	--          the Customer dimension table (only if they don't already exist)
	--
	-- WHY USE INSERT...SELECT?
	--   - More efficient than inserting rows one-by-one
	--   - Reduces network traffic between application and database
	--   - Allows filtering and transformation in a single operation
	--
	-- STAR SCHEMA CONCEPT:
	--   Dim_Customer is a DIMENSION table that stores descriptive attributes
	--   about customers (who, what, where) - these are looked up by fact tables
	
    INSERT INTO Sales.Dim_Customer (CustomerID, CustomerName, EmailAddress)
    SELECT DISTINCT 
        CustomerName,           -- Customer identifier (used as ID)
        CustomerName,           -- Customer name attribute
        EmailAddress            -- Contact information attribute
    FROM [Sales].[Staging_Sales]
    WHERE YEAR(OrderDate) = @OrderYear  -- Filter to specific year only
    
    -- PREVENTS DUPLICATE INSERTION (De-duplication logic)
    -- Check if this customer already exists in the dimension table
    -- This ensures we don't create duplicate customer records
    AND NOT EXISTS (
        SELECT 1                -- '1' is just a placeholder (performance-optimized)
        FROM Sales.Dim_Customer
        WHERE Sales.Dim_Customer.CustomerName = Sales.Staging_Sales.CustomerName
        AND Sales.Dim_Customer.EmailAddress = Sales.Staging_Sales.EmailAddress
    );
    
    -- =====================================================================
    -- STEP 2: LOAD ITEM DIMENSION TABLE
    -- =====================================================================
    -- PURPOSE: Extract unique items/products from staging and insert them 
    --          into the Item dimension table (only if they don't exist)
    --
    -- DIMENSION TABLE PURPOSE:
    --   Stores product/item information that fact tables reference via ItemID
    --   This is a denormalized structure for fast lookups in analytics
    
    INSERT INTO Sales.Dim_Item (ItemID, ItemName)
    SELECT DISTINCT 
        Item,                   -- Item identifier (name used as ID)
        Item                    -- Item name attribute
    FROM [Sales].[Staging_Sales]
    WHERE YEAR(OrderDate) = @OrderYear  -- Only load data for the specified year
    
    -- PREVENTS DUPLICATE INSERTION
    -- Checks if this item already exists before inserting
    AND NOT EXISTS (
        SELECT 1
        FROM Sales.Dim_Item
        WHERE Sales.Dim_Item.ItemName = Sales.Staging_Sales.Item
    );
    
    -- =====================================================================
    -- STEP 3: LOAD SALES FACT TABLE
    -- =====================================================================
    -- PURPOSE: Load all sales transactions for the year into the fact table
    --
    -- FACT TABLE CONCEPT:
    --   Fact_Sales is the central table containing measurable events
    --   (transactions). It stores quantities, amounts, and foreign keys
    --   that reference dimension tables. Facts typically contain:
    --   - Foreign Keys (CustomerID, ItemID) linking to dimensions
    --   - Measures/Metrics (Quantity, TaxAmount, UnitPrice)
    --   - Date information for time-series analysis
    --
    -- NOTE: Unlike dimension loads, we don't check for duplicates here
    --       This could be an issue if staging data has duplicates!
    
    INSERT INTO Sales.Fact_Sales 
    (
        CustomerID,              -- Foreign key to Dim_Customer
        ItemID,                  -- Foreign key to Dim_Item
        SalesOrderNumber,        -- Business transaction identifier
        SalesOrderLineNumber,    -- Line item number within an order
        OrderDate,               -- When the transaction occurred (dimension)
        Quantity,                -- MEASURE: Number of items sold
        TaxAmount,               -- MEASURE: Tax paid
        UnitPrice                -- MEASURE: Price per unit
    )
    SELECT 
        CustomerName,            -- Maps to CustomerID in dimension
        Item,                    -- Maps to ItemID in dimension
        SalesOrderNumber,        -- Order reference number
        CAST(SalesOrderLineNumber AS INT),  -- Convert text to integer
        CAST(OrderDate AS DATE), -- Convert to DATE type (removes time portion)
        CAST(Quantity AS INT),   -- Convert to integer for math operations
        CAST(TaxAmount AS FLOAT),-- Convert to decimal for precision
        CAST(UnitPrice AS FLOAT) -- Convert to decimal for price calculations
    FROM [Sales].[Staging_Sales]
    WHERE YEAR(OrderDate) = @OrderYear;  -- Filter for the specified year
    
    -- =====================================================================
    -- EXECUTION COMPLETE
    -- =====================================================================
    -- DATA WAREHOUSE FLOW SUMMARY:
    -- Staging Table (raw data) 
    --   ↓
    -- This Procedure (transformation & loading)
    --   ↓
    -- Dimension Tables (Dim_Customer, Dim_Item) + Fact Table (Fact_Sales)
    --   ↓
    -- Analytics/Reporting (queries use star schema structure)
    -- =====================================================================

END;

-- =====================================================================
-- EXAMPLE USAGE:
-- =====================================================================
-- Execute the procedure to load 2024 data:
-- EXEC Sales.LoadDataFromStaging @OrderYear = 2024;
--
-- POTENTIAL ISSUES TO FIX:
-- 1. CustomerID should be auto-generated (IDENTITY), not CustomerName
-- 2. Fact table should use actual CustomerID & ItemID foreign keys,
--    not CustomerName and Item names
-- 3. Fact table should have duplicate detection (NOT EXISTS) like dimensions
-- 4. Consider using MERGE instead of INSERT for better error handling
-- =====================================================================
```

    > **Important:** In a data warehouse, foreign key constraints are not always necessary at the table level. While foreign key constraints can help ensure data integrity, they can also add overhead to the ETL (Extract, Transform, Load) process and slow down data loading. The decision to use foreign key constraints in a data warehouse should be based on a careful consideration of the trade-offs between data integrity and performance.

1. In the **Explorer**, navigate to **Schemas >> Sales >> Tables**. Note the *Fact_Sales*, *Dim_Customer*, and *Dim_Item* tables you just created.

    > **Note**: If you can't see the new schemas, open the **...** menu for **Tables** in the **Explorer** pane, then select **Refresh**.

1. Open a new **New SQL query** editor, then copy and run the following query. Update *\<your lakehouse name>* with the lakehouse you created.

    ```sql
    CREATE VIEW Sales.Staging_Sales
    AS
	SELECT * FROM [<your lakehouse name>].[dbo].[staging_sales];
    ```

1. In the **Explorer**, navigate to **Schemas >> Sales >> Views**. Note the *Staging_Sales* view you created.

## Load data to the warehouse

Now that the fact and dimensions tables are created, let's create a stored procedure to load the data from our lakehouse into the warehouse. Because of the automatic SQL endpoint created when we create the lakehouse, you can directly access the data in your lakehouse from the warehouse using T-SQL and cross-database queries.

For the sake of simplicity in this case study, you'll use the customer name and item name as the primary keys.

1. Create a new **New SQL query** editor, then copy and run the following query.

    ```sql
    CREATE OR ALTER PROCEDURE Sales.LoadDataFromStaging (@OrderYear INT)
    AS
    BEGIN
    	-- Load data into the Customer dimension table
        INSERT INTO Sales.Dim_Customer (CustomerID, CustomerName, EmailAddress)
        SELECT DISTINCT CustomerName, CustomerName, EmailAddress
        FROM [Sales].[Staging_Sales]
        WHERE YEAR(OrderDate) = @OrderYear
        AND NOT EXISTS (
            SELECT 1
            FROM Sales.Dim_Customer
            WHERE Sales.Dim_Customer.CustomerName = Sales.Staging_Sales.CustomerName
            AND Sales.Dim_Customer.EmailAddress = Sales.Staging_Sales.EmailAddress
        );
        
        -- Load data into the Item dimension table
        INSERT INTO Sales.Dim_Item (ItemID, ItemName)
        SELECT DISTINCT Item, Item
        FROM [Sales].[Staging_Sales]
        WHERE YEAR(OrderDate) = @OrderYear
        AND NOT EXISTS (
            SELECT 1
            FROM Sales.Dim_Item
            WHERE Sales.Dim_Item.ItemName = Sales.Staging_Sales.Item
        );
        
        -- Load data into the Sales fact table
        INSERT INTO Sales.Fact_Sales (CustomerID, ItemID, SalesOrderNumber, SalesOrderLineNumber, OrderDate, Quantity, TaxAmount, UnitPrice)
        SELECT CustomerName, Item, SalesOrderNumber, CAST(SalesOrderLineNumber AS INT), CAST(OrderDate AS DATE), CAST(Quantity AS INT), CAST(TaxAmount AS FLOAT), CAST(UnitPrice AS FLOAT)
        FROM [Sales].[Staging_Sales]
        WHERE YEAR(OrderDate) = @OrderYear;
    END
    ```
1. Create a new **New SQL query** editor, then copy and run the following query.

    ```sql
    EXEC Sales.LoadDataFromStaging 2021
    ```

    > **Note:** In this case, we are only loading data from the year 2021. However, you have the option to modify it to load data from previous years.

## Run analytical queries

Let's run some analytical queries to validate the data in the warehouse.

1. On the top menu, select **New SQL query**, then copy and run the following query.

    ```sql
    SELECT c.CustomerName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Customer c
    ON s.CustomerID = c.CustomerID
    WHERE YEAR(s.OrderDate) = 2021
    GROUP BY c.CustomerName
    ORDER BY TotalSales DESC;
    ```

    > **Note:** This query shows the customers by total sales for the year of 2021. The customer with the highest total sales for the specified year is **Jordan Turner**, with total sales of **14686.69**. 

1. On the top menu, select **New SQL query** or reuse the same editor, then copy and run the following query.

    ```sql
    SELECT i.ItemName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Item i
    ON s.ItemID = i.ItemID
    WHERE YEAR(s.OrderDate) = 2021
    GROUP BY i.ItemName
    ORDER BY TotalSales DESC;

    ```

    > **Note:** This query shows the top-seliing items by total sales for the year of 2021. These results suggest that the *Mountain-200 bike* model, in both black and silver colors, was the most popular item among customers in 2021.

1. On the top menu, select **New SQL query** or reuse the same editor, then copy and run the following query.

    ```sql
    WITH CategorizedSales AS (
    SELECT
        CASE
            WHEN i.ItemName LIKE '%Helmet%' THEN 'Helmet'
            WHEN i.ItemName LIKE '%Bike%' THEN 'Bike'
            WHEN i.ItemName LIKE '%Gloves%' THEN 'Gloves'
            ELSE 'Other'
        END AS Category,
        c.CustomerName,
        s.UnitPrice * s.Quantity AS Sales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Customer c
    ON s.CustomerID = c.CustomerID
    JOIN Sales.Dim_Item i
    ON s.ItemID = i.ItemID
    WHERE YEAR(s.OrderDate) = 2021
    ),
    RankedSales AS (
        SELECT
            Category,
            CustomerName,
            SUM(Sales) AS TotalSales,
            ROW_NUMBER() OVER (PARTITION BY Category ORDER BY SUM(Sales) DESC) AS SalesRank
        FROM CategorizedSales
        WHERE Category IN ('Helmet', 'Bike', 'Gloves')
        GROUP BY Category, CustomerName
    )
    SELECT Category, CustomerName, TotalSales
    FROM RankedSales
    WHERE SalesRank = 1
    ORDER BY TotalSales DESC;
    ```

    > **Note:** The results of this query show the top customer for each of the categories: Bike, Helmet, and Gloves, based on their total sales. For example, **Joan Coleman** is the top customer for the **Gloves** category.
    >
    > The category information was extracted from the `ItemName` column using string manipulation, as there is no separate category column in the dimension table. This approach assumes that the item names follow a consistent naming convention. If the item names do not follow a consistent naming convention, the results may not accurately reflect the true category of each item.

In this exercise, you have created a lakehouse and a data warehouse with multiple tables. You have ingested data and used cross-database queries to load data from the lakehouse to the warehouse. Additionally, you have used the query tool to perform analytical queries.

## Clean up resources

If you've finished exploring your data warehouse, you can delete the workspace you created for this exercise.

1. In the bar on the left, select the icon for your workspace to view all of the items it contains.
1. Select **Workspace settings** and in the **General** section, scroll down and select **Remove this workspace**.
1. Select **Delete** to delete the workspace.
