-- ====================================================
-- CREATE SCHEMA FOR THE DEMO/TUTORIAL
-- ====================================================

CREATE SCHEMA dp700_e014;


-- ====================================================
-- SCD TYPE 1: Overwrite Existing Data (No History)
-- ====================================================

-- Create initial dim_product_scd1 table
CREATE TABLE dp700_e014.dim_product_scd1 (
    product_id INT,
    product_name VARCHAR(100),
    price DECIMAL(10,2)
);

-- Insert initial product data
INSERT INTO dp700_e014.dim_product_scd1 (product_id, product_name, price)
VALUES
    (1, 'Laptop', 1200.00),
    (2, 'Headphones', 150.00);


SELECT *
FROM dp700_e014.dim_product_scd1;


-- Update product price
UPDATE dp700_e014.dim_product_scd1
SET price = 1300.00
WHERE product_id = 1;

SELECT *
FROM dp700_e014.dim_product_scd1;



-- ====================================================
-- SCD TYPE 2: Add New Record for Each Change (Full History)
-- ====================================================

CREATE TABLE dp700_e014.dim_product_scd2 (
    surrogate_key INT,
    product_id INT,
    product_name VARCHAR(100),
    price DECIMAL(10,2),
    record_start_date DATE,
    record_end_date DATE,
    is_current BIT
);

INSERT INTO dp700_e014.dim_product_scd2 (surrogate_key, product_id, product_name, price, record_start_date, record_end_date, is_current)
VALUES
    (101, 1, 'Laptop', 1200.00, '2025-01-01', '2999-12-31', 1),
    (102, 2, 'Headphones', 150.00, '2025-01-01', '2999-12-31', 1);

SELECT *
FROM dp700_e014.dim_product_scd2;

-- Step 1: Set current record as inactive
UPDATE dp700_e014.dim_product_scd2
SET record_end_date = CAST(GETDATE() AS DATE), is_current = 0
WHERE product_id = 1 AND is_current = 1;

-- Step 2: Insert new active record
INSERT INTO dp700_e014.dim_product_scd2 (surrogate_key, product_id, product_name, price, record_start_date, record_end_date, is_current)
VALUES
    (103, 1, 'Laptop', 1300.00, CAST(GETDATE() AS DATE), '2999-12-31', 1);

SELECT *
FROM dp700_e014.dim_product_scd2
ORDER BY surrogate_key;

-- ====================================================
-- SCD TYPE 3: Track Previous Value
-- ====================================================

CREATE TABLE dp700_e014.dim_product_scd3 (
    product_id INT,
    product_name VARCHAR(100),
    current_price DECIMAL(10,2),
    previous_price DECIMAL(10,2),
    current_valid_from DATE
);

INSERT INTO dp700_e014.dim_product_scd3 (product_id, product_name, current_price, previous_price, current_valid_from)
VALUES
    (1, 'Laptop', 1200.00, NULL, '2025-01-01'),
    (2, 'Headphones', 150.00, NULL, '2025-01-01');


SELECT *
FROM dp700_e014.dim_product_scd3

-- Update price and move current price to previous price
UPDATE dp700_e014.dim_product_scd3
SET previous_price = current_price,
    current_price = 1300.00,
    current_valid_from = CAST(GETDATE() AS DATE)
WHERE product_id = 1;

SELECT *
FROM dp700_e014.dim_product_scd3
