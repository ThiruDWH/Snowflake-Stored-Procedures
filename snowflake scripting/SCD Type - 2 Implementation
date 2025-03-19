-- Create the database
CREATE DATABASE IF NOT EXISTS mydb;

-- Create schemas for staging and target
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS target;

-- Create a staging table to temporarily hold employee data
CREATE OR REPLACE TABLE staging.stg_empl (
  eid INTEGER NOT NULL,
  ename VARCHAR(30), 
  dob DATE, 
  mail VARCHAR(255), 
  phone VARCHAR(20), 
  salary INTEGER,
  dept VARCHAR(30),
  loc VARCHAR(30), 
  PRIMARY KEY (eid)
);

-- Create the target table for slowly changing dimension Type 1 (SCD1)
CREATE OR REPLACE TABLE target.empl_scd2 (
  emp_id INTEGER NOT NULL, 
  emp_name VARCHAR(30), 
  date_of_birth DATE, 
  email VARCHAR(255), 
  phone_number VARCHAR(20), 
  salary INTEGER, -- Salary
  department VARCHAR(30), 
  work_location VARCHAR(30), 
  effective_datetime TIMESTAMP,
  expire_datetime TIMESTAMP,
  PRIMARY KEY (emp_id, effective_datetime)
);

--Create streams on stage table
CREATE STREAM STAGING.STREAM_STG_EMPL_U ON TABLE staging.stg_empl;
CREATE STREAM STAGING.STREAM_STG_EMPL_I ON TABLE staging.stg_empl;

--Create a procedure to implement Type 2 logic
CREATE OR REPLACE PROCEDURE EMP.TARGET.PROC_EMPL_SCD_TYPE2()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS

  DECLARE 
   cur_ts TIMESTAMP;

  BEGIN

  cur_ts := CURRENT_TIMESTAMP();

--Expire the old records and insert new records
  MERGE INTO target.empl_scd2 TGT
  USING STAGING.STREAM_STG_EMPL_U STR
  ON TGT.emp_id = STR.eid
  AND TGT.expire_datetime is NULL

  WHEN MATCHED
    AND STR.METADATA$ACTION ='DELETE'
    AND STR.METADATA$ISUPDATE ='TRUE'
    THEN
    UPDATE SET TGT.expire_datetime = :cur_ts

  WHEN NOT MATCHED THEN
    INSERT (emp_id, emp_name, date_of_birth, email, phone_number, salary, department, work_location, effective_datetime, expire_datetime)
    VALUES (STR.eid, STR.ename, STR.dob, STR.mail, STR.phone, STR.salary, STR.dept, STR.loc, :cur_ts, null);

--Insert the new version of old records
    INSERT INTO target.empl_scd2
    (emp_id, emp_name, date_of_birth, email, phone_number, salary, department, work_location, effective_datetime, expire_datetime)
    SELECT eid, ename, dob, mail, phone, salary, dept, loc, :cur_ts, null
    FROM STAGING.STREAM_STG_EMPL_I
    WHERE METADATA$ACTION ='INSERT' AND METADATA$ISUPDATE ='TRUE';

    RETURN 'Proc completed successfully';

    END;

--CALL EMP.TARGET.PROC_EMPL_SCD_TYPE2();

--Schedule the procedure
CREATE OR REPLACE TASK TARGET.TASK_EMPL_DATA_LOAD2
    SCHEDULE = '2 MINUTES'
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_STG_EMPL_U')
  AS
  CALL EMP.TARGET.PROC_EMPL_SCD_TYPE2();

//Start the task
ALTER TASK TARGET.TASK_EMPL_DATA_LOAD2 RESUME;

-- Show all tasks to check their status
SHOW TASKS;

-- Insert sample data into staging table
INSERT INTO STAGING.STG_EMPL VALUES
    (1, 'Rahul Sharma', '1986-04-15', 'rahul.sharma@gmail.com', '9988776655', 92000, 'Administration', 'Bangalore'),
    (2, 'Renuka Devi', '1993-10-19', 'renuka1993@yahoo.com', '+91 9911882255', 61000, 'Sales', 'Hyderabad'),
    (3, 'Kamalesh', '1991-02-08', 'kamal91@outlook.com', '9182736450', 59000, 'Sales', 'Chennai'),
    (4, 'Arun Kumar', '1989-05-20', 'arun_kumar@gmail.com', '901-287-3465', 74500, 'IT', 'Bangalore');

-- Observe the streams now with change capture
SELECT * FROM STAGING.STREAM_STG_EMPL_U;
SELECT * FROM STAGING.STREAM_STG_EMPL_I;

-- Manually execute the task to apply changes
EXECUTE TASK TARGET.TASK_EMPL_DATA_LOAD2;

--After 1st run
--Verify the data in Target table
SELECT * FROM target.empl_scd2;

--Observe the streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL_U;
SELECT * FROM STAGING.STREAM_STG_EMPL_I;

-------------------------------------------

-- Make changes to the stage table (Assume it is truncate and load with new add updated records)
INSERT INTO STAGING.STG_EMPL VALUES
    (5, 'Arjun', '1989-05-20', 'arjun_kumar@gmail.com', '9703408236', 74000, 'IT', 'Rajoli');

-- Update an existing employee record to test update logic
UPDATE STAGING.STG_EMPL SET salary = 2000 , phone = '9911882255' WHERE eid = 2;

--Observe the streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL_U;
SELECT * FROM STAGING.STREAM_STG_EMPL_I;

--After 2 minutes (task willbe running for every 2 minutes)
--Verify the data in Target table
SELECT * FROM target.empl_scd2;

--Observe the streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL_U;
SELECT * FROM STAGING.STREAM_STG_EMPL_I;

--------------------------------------------------------

--One more time make changes to the stage table (Assume it is truncate and load with new add updated records)

INSERT INTO STAGING.STG_EMPL VALUES
    (6, 'kiran', '1989-05-20', 'kiran_kumar@gmail.com', '8247078947', 57000, 'Administration', 'ieeja');

UPDATE STAGING.STG_EMPL SET phone = '9911882255', dept = 'it' WHERE eid = 2;
UPDATE  STAGING.STG_EMPL SET phone = '9012873465', dept = 'Administration' WHERE eid = 4;

--Observe the streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL_U;
SELECT * FROM STAGING.STREAM_STG_EMPL_I;

--After 2 minutes (task willbe running for every 2 minutes)
--Verify the data in Target table
SELECT * FROM target.empl_scd2;

--Observe the streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL_U;
SELECT * FROM STAGING.STREAM_STG_EMPL_I;

---------------------
--Stop or drop the task, otherwise all your free credits will be consumed
ALTER TASK TARGET.TASK_EMPL_DATA_LOAD2 SUSPEND;
