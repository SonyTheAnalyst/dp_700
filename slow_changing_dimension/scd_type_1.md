<img width="1326" height="296" alt="Image" src="https://github.com/user-attachments/assets/be2fc789-8248-4a88-b9ed-2100cf282bbf" />

# The script 2 activity

<img width="652" height="186" alt="Image" src="https://github.com/user-attachments/assets/dc59380a-5d02-4628-882b-700e16a315ad" />

# Get meta data activity

<img width="968" height="257" alt="Image" src="https://github.com/user-attachments/assets/d69b26b7-0ea3-451e-a9a1-e90aeac8a7e7" />

# for each 
items = @activity('Get Metadata1').output.childItems
## for each : copy data

<img width="691" height="257" alt="Image" src="https://github.com/user-attachments/assets/446b21d8-62bd-4631-80d7-9792d9e0a41a" />

## for each : set variables
value = @formatDateTime(substring(item().name, 9, 10), 'yyyy-MM-dd')

<img width="553" height="169" alt="Image" src="https://github.com/user-attachments/assets/cb49ff26-92b9-4609-aa1f-8ac8584e61c6" />

## for each : script1

<img width="654" height="230" alt="Image" src="https://github.com/user-attachments/assets/8298db12-8dce-49b5-a84c-55937b3d5f0d" />

# Store procedure

<img width="665" height="173" alt="Image" src="https://github.com/user-attachments/assets/f2df68bb-ddc2-4151-8e25-232183469e5d" />

sql
'''

          CREATE OR ALTER PROCEDURE sp_upsert_dim_customer_scd1
          AS
          BEGIN
              SET NOCOUNT ON;
          
              -- Update existing records with newer data
              UPDATE tgt
              SET 
                  name = src.name,
                  email = src.email,
                  address = src.address,
                  last_updated_date = src.file_date
              FROM dim_customer tgt
              INNER JOIN stg_customer src
                  ON tgt.customer_id = src.customer_id
              WHERE TRY_CAST(src.file_date as DATE) > TRY_CAST(tgt.last_updated_date as DATE)
                AND (
                      tgt.name <> src.name OR
                      tgt.email <> src.email OR
                      tgt.address <> src.address
                );
          
              -- Insert new records that don't exist
              INSERT INTO dim_customer (customer_id, name, email, address, last_updated_date)
              SELECT customer_id, name, email, address, file_date
              FROM stg_customer src
          
              WHERE NOT EXISTS ( -- is a standard, high-performance way to identify new records for insertion without creating duplicate rows.
              -- tells the database to only insert a new record if its unique ID cannot already be found in the target table.
                  SELECT 1 
                  FROM dim_customer tgt
                  WHERE tgt.customer_id = src.customer_id
              );
          END;

'''







