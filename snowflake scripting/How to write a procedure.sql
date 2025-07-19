-- Create the database EMP
CREATE DATABASE EMP;

-- Create schema for procedures
CREATE SCHEMA EMP.PROCS;

-- Create schema for HR-related data
CREATE SCHEMA EMP.HRDATA;

-- Creating a stored procedure that returns a greeting message
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_SAMPLE_PROGRAM ("N" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 
BEGIN 
  -- Concatenating input name with a message
  RETURN 'YOUR GOOD NAME IS ' || N;
END;

-- Calling the stored procedure with 'THIRU' as input
CALL EMP.PROCS.SP_SAMPLE_PROGRAM ('THIRU');

-- Replacing the previous stored procedure with a different message format
-- This will overwrite the earlier version
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_SAMPLE_PROGRAM ("N" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER 
AS 
BEGIN 
  -- Concatenating input name with another message
  RETURN 'MY NAME IS ' || N;
END;

-- Calling the updated stored procedure with 'THIRU' as input
CALL EMP.PROCS.SP_SAMPLE_PROGRAM ('THIRU');

-- Creating a stored procedure to compute the square root of a given number
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_FIND_SQRT ("IP_NUMBER" INTEGER)
RETURNS INTEGER  -- Note: This may cause truncation if the result is a decimal
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
  SQROOT INTEGER;  -- Variable to store the square root (consider using DECIMAL for precision)
BEGIN
  -- Computing the square root of the input number
  SQROOT := SQRT(IP_NUMBER);
  RETURN SQROOT;
END;

-- Calling the square root procedure with an integer
CALL EMP.PROCS.SP_FIND_SQRT (9398134);

-- Calling the procedure with a string (This will cause an error due to type mismatch)
CALL EMP.PROCS.SP_FIND_SQRT ('83749813948631949');  -- ERROR: Expected INTEGER, received STRING

-- Calling the procedure with a perfect square number
CALL EMP.PROCS.SP_FIND_SQRT(625);  -- Expected output: 25
