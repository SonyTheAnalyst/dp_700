
'''sql

CREATE SCHEMA dp_700;
GO
DROP TABLE dbo.employee;
GO
-- ----------------------------
CREATE TABLE dp_700.employee(
	employee_id INT NULL,
	name VARCHAR(50) NULL ,
	departmenent_id INT NULL,
	hire_date DATE NULL,
	salary FLOAT NULL

)

SELECT*
FROM dp_700.employee;



INSERT INTO dp_700.employee(employee_id, name, departmenent_id,  hire_date, salary)
VALUES
	(1, 'john doe', 101, '2025-06-01', 75000.00),
	(2, 'jane Smith', 102, '2018-09-15', 85000.00),
	(3, 'Alex johnson', 103, '2020-01-10', 68000.00),
	(4, 'Sara Lee', 101, '2017-03-12', 72000.00),
	(5, 'Michael Brown', 102, '2019-11-20', 95000.00),
	(6, 'Emma Davis', 103, '2021-07-18', 62000.00);
GO 

SELECT* FROM dp_700.employee
'''



  '''sql

CREATE FUNCTION employee_function (@employee_id INT)
RETURNS TABLE
AS 
RETURN
(
	SELECT*
	FROM dp_700.employee
	WHERE employee_id = @employee_id
)

SELECT* FROM employee_function(2)

CREATE FUNCTION dp_700.CalculateAnnualBonus (
    @salary DECIMAL(18, 2), 
    @performance_rating INT
)
RETURNS DECIMAL(18, 2)
AS
BEGIN
    -- Returns 10% if rating is 5, else 5%
    RETURN (CASE WHEN @performance_rating = 5 THEN @salary * 0.10 ELSE @salary * 0.05 END);
END;
GO
'''



'''sql

-- How to use it:
SELECT name, salary, dp_700.CalculateAnnualBonus(salary, 5) AS bonus
FROM dp_700.employee;
'''




'''sql

CREATE FUNCTION dp_700.GetHighEarnersByDept (
    @dept_id INT, 
    @min_salary DECIMAL(18, 2)
)
RETURNS TABLE
AS
RETURN (
    SELECT 
        employee_id, 
        name, 
        salary
    FROM dp_700.employee
    WHERE departmenent_id = @dept_id 
      AND salary >= @min_salary
);
GO
'''

-- How to use it:
SELECT * FROM dp_700.GetHighEarnersByDept(102, 80000);


3. The Date-Logic Function (Tenure Analysis)
This example calculates how many years an employee has been with the company based on the hire_date column visible in your screenshot.

  
'''sql
CREATE FUNCTION dp_700.GetEmployeeTenure (@emp_id INT)
RETURNS TABLE
AS
RETURN (
    SELECT 
        name,
        hire_date,
        DATEDIFF(year, hire_date, GETDATE()) AS years_employed
    FROM dp_700.employee
    WHERE employee_id = @emp_id
);
GO

-- How to use it:
SELECT * FROM dp_700.GetEmployeeTenure(2);

'''













