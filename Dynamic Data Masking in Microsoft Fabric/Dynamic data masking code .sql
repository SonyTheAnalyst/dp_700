# Dynamic data masking code

CREATE SCHEMA dp700_e019;
GO

CREATE TABLE dp700_e019.masking_demo (
    user_id INT,
    full_name VARCHAR(100),
    short_text VARCHAR(3) MASKED WITH (FUNCTION = 'default()'),
    notes VARCHAR(100) MASKED WITH (FUNCTION = 'default()'),
    email_address VARCHAR(100) MASKED WITH (FUNCTION = 'email()'),
    credit_card_number VARCHAR(20) MASKED WITH (FUNCTION = 'partial(0,"XXXX-XXXX-XXXX-",4)'),
    custom_id VARCHAR(20) MASKED WITH (FUNCTION = 'partial(2,"-MASKED-",2)'),
    random_code VARCHAR(20) MASKED WITH (FUNCTION = 'partial(0,"XXXXXX",0)'),
    salary DECIMAL(10,2) MASKED WITH (FUNCTION = 'default()'),
    birth_date DATE MASKED WITH (FUNCTION = 'default()'),
    access_code VARBINARY(8) MASKED WITH (FUNCTION = 'default()'),
    random_number INT MASKED WITH (FUNCTION = 'random(1000, 9999)')
);
GO

  
INSERT INTO dp700_e019.masking_demo (
    user_id, full_name, short_text, notes, email_address, credit_card_number,
    custom_id, random_code, salary, birth_date, access_code, random_number
)
VALUES
(1, 'Alice Johnson', 'Yes', 'Secret', 'alice@company.com', '1234-5678-9012-3456',
 'USR1234567890', 'XYZ789', 72000.00, '1990-05-15', 0x5A5A5A5A5A5A5A5A, 1234),
(2, 'Bob', 'OK', 'Top', 'bob@fabrikam.net', '9876-5432-1111-2222',
 'EMP8765432100', 'CODE42', 55000.00, '1985-09-22', 0x6B6B6B6B6B6B6B6B, 5678),
(3, 'Charlie', 'No', 'Internal', 'charlie@abc.org', '1111-2222-3333-4444',
 'ID0000009999', 'RND123', 43000.00, '2000-01-01', 0x7C7C7C7C7C7C7C7C, 9012),
(4, 'Diana Lee', 'Hi', 'Dev', 'd', '4444-3333-2222-1111',
 'DL9988776655', 'ALPHA1', 66000.00, '1993-02-20', 0x1111111111111111, 8452),
(5, 'Eve', 'No', 'Temp', 'e@x.org', '1234-4321-5678-8765',
 'TEM', 'BETA9', 39000.00, '1999-07-07', 0x2222222222222222, 4567),
(6, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL),
(7, 'Grace Hopper', '', 'Legend', '', '1357-2468-1357-2468',
 '', 'DELTA4', 95000.00, '1906-12-09', 0x4444444444444444, 9823),
(8, 'Henry', NULL, 'QA', 'this is not an email', '2468-1357-2468-1357',
 'H', 'ZETA3', 47000.00, '1988-03-11', 0x5555555555555555, 1289);
GO

GRANT SELECT ON dp700_e019.masking_demo TO [aleksi.partanen.2@naksfi.onmicrosoft.com];
GO

/*********************************************************************/

SELECT *
FROM dp700_e019.masking_demo
ORDER BY user_id

/*********************************************************************/

ALTER TABLE dp700_e019.masking_demo
ALTER COLUMN full_name ADD MASKED WITH (FUNCTION = 'default()');

/*********************************************************************/

ALTER TABLE dp700_e019.masking_demo
ALTER COLUMN full_name DROP MASKED;

/*********************************************************************/

GRANT UNMASK ON dp700_e019.masking_demo TO [aleksi.partanen.2@naksfi.onmicrosoft.com];
GO

/*********************************************************************/

REVOKE UNMASK ON dp700_e019.masking_demo TO [aleksi.partanen.2@naksfi.onmicrosoft.com];
GO

/*********************************************************************/


SELECT *
FROM dp700_e019.masking_demo
WHERE salary>=65000 AND salary<=75000
ORDER BY user_id


SELECT *
FROM dp700_e019.masking_demo
WHERE custom_id = 'USR1234567890'
ORDER BY user_id

/*********************************************************************/

/* CLEAN UP
DROP TABLE dp700_e019.masking_demo;
GO

DROP SCHEMA dp700_e019;
GO
*/
