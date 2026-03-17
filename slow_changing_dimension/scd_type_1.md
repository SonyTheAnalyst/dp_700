
<img width="1326" height="296" alt="Image" src="https://github.com/user-attachments/assets/be2fc789-8248-4a88-b9ed-2100cf282bbf" />

This pipeline demonstrates a **Slowly Changing Dimension (SCD) Type 1** load for `dim_customer`.
SCD Type 1 means we **overwrite** customer attributes (no history preserved). We keep a `last_updated_date` so we only update when a newer file arrives.

---

## The script 2 activity

<img width="652" height="186" alt="Image" src="https://github.com/user-attachments/assets/dc59380a-5d02-4628-882b-700e16a315ad" />

Purpose: prepares/controls the flow before the per-file processing (based on your pipeline design).

---

## Get meta data activity

<img width="968" height="257" alt="Image" src="https://github.com/user-attachments/assets/d69b26b7-0ea3-451e-a9a1-e90aeac8a7e7" />

Purpose: reads the folder and returns the list of files to process.

---

## For each

**Items expression:**
```text
@activity('Get Metadata1').output.childItems
```

### for each : copy data

<img width="691" height="257" alt="Image" src="https://github.com/user-attachments/assets/446b21d8-62bd-4631-80d7-9792d9e0a41a" />

Purpose: copies each file’s rows into the staging table (example: `stg_customer`).

### for each : set variables

**Extract file date from filename and format as `yyyy-MM-dd`:**
```text
@formatDateTime(substring(item().name, 9, 10), 'yyyy-MM-dd')
```

<img width="553" height="169" alt="Image" src="https://github.com/user-attachments/assets/cb49ff26-92b9-4609-aa1f-8ac8584e61c6" />

> Assumption: your file naming convention contains the date at position `9` for length `10`
> (example pattern like `customer_2026-03-17.csv`).

### for each : script1

<img width="654" height="230" alt="Image" src="https://github.com/user-attachments/assets/8298db12-8dce-49b5-a84c-55937b3d5f0d" />

Purpose: executes SQL logic (or triggers the stored procedure) to apply SCD Type 1 changes to the dimension.

---

## Store procedure

<img width="665" height="173" alt="Image" src="https://github.com/user-attachments/assets/f2df68bb-ddc2-4151-8e25-232183469e5d" />

```sql
CREATE OR ALTER PROCEDURE sp_upsert_dim_customer_scd1
AS
BEGIN
    SET NOCOUNT ON;

    -- Update existing records only when the incoming file is newer AND values actually changed
    UPDATE tgt
    SET 
        name = src.name,
        email = src.email,
        address = src.address,
        last_updated_date = src.file_date
    FROM dim_customer tgt
    INNER JOIN stg_customer src
        ON tgt.customer_id = src.customer_id
    WHERE TRY_CAST(src.file_date AS DATE) > TRY_CAST(tgt.last_updated_date AS DATE)
      AND (
            tgt.name <> src.name OR
            tgt.email <> src.email OR
            tgt.address <> src.address
      );

    -- Insert new customers not present in the dimension
    INSERT INTO dim_customer (customer_id, name, email, address, last_updated_date)
    SELECT customer_id, name, email, address, file_date
    FROM stg_customer src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_customer tgt
        WHERE tgt.customer_id = src.customer_id
    );
END;
```
```
