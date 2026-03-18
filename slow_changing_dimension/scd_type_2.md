<img width="1007" height="293" alt="image" src="https://github.com/user-attachments/assets/2a6de74c-e86e-4830-a4e1-0d7cf86f7093" />

# script activity
<img width="654" height="182" alt="image" src="https://github.com/user-attachments/assets/e0c5c4b5-5427-4af1-b742-571b6808e059" />


# Get metadata
<img width="1083" height="256" alt="image" src="https://github.com/user-attachments/assets/ff8f188c-db31-4547-95c8-cf424d788c91" />

# For each activity 
```
@activity('Get Metadata1').output.childItems
```
# For each : copy data

<img width="1140" height="298" alt="image" src="https://github.com/user-attachments/assets/0eca7814-94b0-4bf2-b667-3a995496d3fd" />

<img width="714" height="252" alt="image" src="https://github.com/user-attachments/assets/af8321c2-1af7-434a-ae5f-9ff15c918674" />

# Store procedure

```
CREATE OR ALTER PROCEDURE dbo.sp_upsert_customer_scd2
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------
    -- Step 1 & 2: Update existing record if values have changed
    ----------------------------------------------------------------
    WITH latest_customer AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
            FROM stg_customer
        ) ranked
        WHERE rn = 1
    )
    UPDATE dc
    SET end_date = lc.last_updated,
        is_current = 0
    FROM dim_customer dc
    JOIN latest_customer lc
      ON dc.customer_id = lc.customer_id
     AND dc.end_date IS NULL
     AND (
         dc.name <> lc.name OR
         dc.address <> lc.address OR
         dc.phone <> lc.phone
     );

    ----------------------------------------------------------------
    -- Step 3: Insert new version or new customers
    ----------------------------------------------------------------
    -- You must define the CTE again here for the INSERT statement
    WITH latest_customer AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
            FROM stg_customer
        ) ranked
        WHERE rn = 1
    )
    INSERT INTO dim_customer (
        customer_id, name, address, phone,
        start_date, end_date, is_current
    )
    SELECT lc.customer_id, lc.name, lc.address, lc.phone,
           lc.last_updated, NULL, 1
    FROM latest_customer lc
    LEFT JOIN dim_customer dc
      ON lc.customer_id = dc.customer_id AND dc.end_date IS NULL
    WHERE dc.customer_id IS NULL
       OR (
           dc.name <> lc.name OR
           dc.address <> lc.address OR
           dc.phone <> lc.phone
       );
END;

```
