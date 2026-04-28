DROP TABLE [warehouse_1].[dbo].[dummy_table]
GO

CREATE TABLE [warehouse_1].[dbo].[dummy_table]
(
	[Country] [varchar](255) NULL,
	[Region] [varchar](255) NULL,
	[Happiness_Rank] [INT] NOT NULL
)
GO



-- COPY INTO COMMAND
COPY INTO [warehouse_1].[dbo].[dummy_table]
FROM 'https://warehousestorageaccount.blob.core.windows.net/container/2015.csv'
WITH
(
FILE_TYPE = 'CSV',
FIRSTROW = 2 -- THIS TELLS SQL TO START AT THE DATA, NOT THE HEADER
)

SELECT*
FROM dummy_table;


------- COPY TABLE AS SELECT CTAS

USE warehouse_1
GO
CREATE SCHEMA GOLD

CREATE TABLE GOLD.gold_region AS
SELECT* FROM dbo.dummy_table --we could also use a lakehouse endpoint imported from the + warehouse
