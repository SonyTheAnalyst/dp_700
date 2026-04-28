-- Create a schema for organizing your objects
CREATE SCHEMA dp700_e013;
GO

-- Create the employees table
CREATE TABLE dp700_e013.employees (
    employee_id INT,
    name VARCHAR(50),
    department_id INT,
    hire_date DATE,
    salary DECIMAL(10, 2)
);
GO

-- Insert initial data into the employees table
INSERT INTO dp700_e013.employees (employee_id, name, department_id, hire_date, salary)
VALUES
    (1, 'John Doe', 101, '2015-06-01', 75000.00),
    (2, 'Jane Smith', 102, '2018-09-15', 85000.00),
    (3, 'Alex Johnson', 103, '2020-01-10', 68000.00),
    (4, 'Sara Lee', 101, '2017-03-12', 72000.00),
    (5, 'Michael Brown', 102, '2019-11-20', 95000.00),
    (6, 'Emma Davis', 103, '2021-07-18', 62000.00);
GO


/******************************************************************/
-- Create a view that adds calculated columns (years employed and seniority level)

CREATE VIEW dp700_e013.vw_employees AS
SELECT 
    employee_id,
    name,
    department_id,
    hire_date,
    salary,
    DATEDIFF(YEAR, hire_date, GETDATE()) AS years_employed,
    CASE 
        WHEN DATEDIFF(YEAR, hire_date, GETDATE()) >= 5 THEN 'Senior'
        WHEN DATEDIFF(YEAR, hire_date, GETDATE()) >= 2 THEN 'Mid-Level'
        ELSE 'Junior'
    END AS seniority_level
FROM dp700_e013.employees;
GO

/******************************************************************/
-- Query the view to display employee data with calculated columns

SELECT *
FROM dp700_e013.vw_employees;

/******************************************************************/
-- Create a stored procedure to return employees by seniority level (with optional department filter)

CREATE PROCEDURE dp700_e013.get_employees_by_seniority
    @seniority_level VARCHAR(20),
    @department_id INT = NULL
AS
BEGIN
    SELECT 
        employee_id,
        name,
        department_id,
        hire_date,
        salary,
        years_employed,
        seniority_level
    FROM dp700_e013.vw_employees
    WHERE seniority_level = @seniority_level
      AND (@department_id IS NULL OR department_id = @department_id);
END;
GO

/******************************************************************/
-- Execute the stored procedure with different parameter sets

EXEC dp700_e013.get_employees_by_seniority @seniority_level = 'Senior';

EXEC dp700_e013.get_employees_by_seniority @seniority_level = 'Senior', @department_id = 102;


/******************************************************************/
-- Load additional employee data from Azure Blob Storage using COPY INTO

COPY INTO dp700_e013.employees
FROM 'https://apfabricstdldev.blob.core.windows.net/dp-700/dp700_e013/employee.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIELDTERMINATOR = ',',
    FIRSTROW = 2
);
GO

/******************************************************************/
-- Query the view again after loading new data

SELECT *
FROM dp700_e013.vw_employees
ORDER BY employee_id;

/******************************************************************/
-- Use time travel to view the view as it existed at a specific point in time
-- Note that we are using a view here!

SELECT *
FROM dp700_e013.employees
ORDER BY employee_id
OPTION (FOR TIMESTAMP AS OF '2025-04-18T19:39:35.28');

SELECT *
FROM dp700_e013.vw_employees
ORDER BY employee_id
OPTION (FOR TIMESTAMP AS OF '2025-04-18T19:39:35.28');

/******************************************************************/
-- Query and compare data across a warehouse and a lakehouse
SELECT *
FROM [wh_dp700].[dp700_e013].[employees]
UNION ALL
SELECT *
FROM [lh_dp700].[dp700_e013].[employees]

/******************************************************************/
-- Insert data from the lakehouse into the warehouse

INSERT INTO [wh_dp700].[dp700_e013].[employees]
SELECT * FROM [lh_dp700].[dp700_e013].[employees];

/******************************************************************/
-- Clone the employees table (optionally at a point in time)

CREATE TABLE [dp700_e013].[employees_clone] AS CLONE OF [dp700_e013].[employees];
--CREATE TABLE [dp700_e013].[employees_clone] AS CLONE OF [dp700_e013].[employees] AT '2025-04-19T14:24:10.325';

/******************************************************************/
-- Query the cloned table

SELECT *
FROM [dp700_e013].[employees_clone]
ORDER BY employee_id DESC

/******************************************************************/
-- Insert a new row to simulate an additional hire

INSERT INTO dp700_e013.employees (employee_id, name, department_id, hire_date, salary)
VALUES
    (12, 'John Smith', 101, '2015-06-01', 78000.00)
GO

/******************************************************************/
-- Query the table to see the new row

SELECT *
FROM [dp700_e013].[employees]
ORDER BY employee_id DESC

/******************************************************************/
-- Truncate the table (simulate accidental data loss)

TRUNCATE TABLE [dp700_e013].[employees];

/******************************************************************/
-- Restore data from the cloned table after truncate

INSERT INTO dp700_e013.employees
SELECT *
FROM dp700_e013.employees_clone

/******************************************************************/
-- Verify the restore by querying the employees table

SELECT *
FROM dp700_e013.employees

/******************************************************************/
-- Use time travel again to view the state before truncation

SELECT *
FROM dp700_e013.employees
ORDER BY employee_id
OPTION (FOR TIMESTAMP AS OF '2025-04-19T12:29:00.00');
