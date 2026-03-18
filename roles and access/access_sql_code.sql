-- Create schema
CREATE SCHEMA dp700_e018;
GO

-- Create employees table
CREATE TABLE dp700_e018.employees (
    employee_id INT,
    full_name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(18, 2)
);

INSERT INTO dp700_e018.employees
VALUES 
(1, 'Alice Johnson', 'HR', 70000),
(2, 'Bob Smith', 'IT', 85000),
(3, 'Carol Lee', 'Sales', 65000);

-- Create sales table
CREATE TABLE dp700_e018.sales (
    sale_id INT,
    region VARCHAR(50),
    sales_person VARCHAR(100),
    amount DECIMAL(18, 2)
);

INSERT INTO dp700_e018.sales
VALUES 
(101, 'north', 'Alice Johnson', 10000),
(102, 'south', 'Bob Smith', 12000),
(103, 'north', 'Carol Lee', 8000),
(104, 'west', 'David Lee', 15000);

-- Create finance table
CREATE TABLE dp700_e018.finance (
    finance_id INT,
    department VARCHAR(50),
    budget DECIMAL(18, 2)
);

INSERT INTO dp700_e018.finance
VALUES 
(1, 'HR', 300000),
(2, 'IT', 500000),
(3, 'Sales', 250000);

-- Create user-region mapping (for RLS)
CREATE TABLE dp700_e018.user_region (
    user_email VARCHAR(100),
    region VARCHAR(50)
);

INSERT INTO dp700_e018.user_region
VALUES
('aleksi.partanen.2@naksfi.onmicrosoft.com', 'north');


/**************************************************************************************/

--Granting access on table level (OLS)
GRANT SELECT ON dp700_e018.finance TO [aleksi.partanen.2@naksfi.onmicrosoft.com];
GRANT SELECT ON dp700_e018.sales TO [aleksi.partanen.2@naksfi.onmicrosoft.com];
GRANT SELECT ON dp700_e018.user_region TO [aleksi.partanen.2@naksfi.onmicrosoft.com];

--Grating access on column level (CLS)
GRANT SELECT ON dp700_e018.employees(employee_id, full_name, department) TO [aleksi.partanen.2@naksfi.onmicrosoft.com];

--Limiting access on column level (RLS)
CREATE FUNCTION dp700_e018.fn_sales_rls(@region VARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS result
WHERE @region IN (
    SELECT region FROM dp700_e018.user_region
    WHERE user_email = USER_NAME()
);
GO

CREATE SECURITY POLICY dp700_e018.sales_rls_policy
ADD FILTER PREDICATE dp700_e018.fn_sales_rls(region)
ON dp700_e018.sales
WITH (STATE = ON);
GO


/**************************************************************************************/



-- TUTORIAL CLEAN UP
/*
DROP TABLE [wh_dp700].[dp700_e018].[employees]
GO

DROP TABLE [wh_dp700].[dp700_e018].[finance]
GO

DROP SECURITY POLICY dp700_e018.sales_rls_policy
GO

DROP FUNCTION dp700_e018.fn_sales_rls
GO

DROP TABLE [wh_dp700].[dp700_e018].[sales]
GO

DROP TABLE [wh_dp700].[dp700_e018].[user_region]
GO

DROP SCHEMA [dp700_e018]
GO
*/

