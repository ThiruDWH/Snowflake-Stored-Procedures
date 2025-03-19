-- Create the database
CREATE DATABASE mydb;

-- Create schemas for staging and target
CREATE SCHEMA staging;
CREATE SCHEMA target;

-- Create a staging table to temporarily hold employee data
CREATE TABLE staging.stg_empl (
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
CREATE TABLE target.empl_scd1 (
  emp_id INTEGER NOT NULL, 
  emp_name VARCHAR(30), 
  date_of_birth DATE, 
  email VARCHAR(255), 
  phone_number VARCHAR(20), 
  salary INTEGER, -- Salary
  department VARCHAR(30), 
  work_location VARCHAR(30), 
  insert_ts TIMESTAMP,
  lat_update_ts TIMESTAMP, 
  PRIMARY KEY (emp_id)
);

-- Create a stream to track changes on the staging table
CREATE OR REPLACE STREAM staging.stream_stg_empl ON TABLE staging.stg_empl;

MERGE INTO target.empl_scd1 trg
USING staging.stream_stg_empl str
ON trg.emp_id = str.eid

WHEN MATCHED 
    AND str.metadata$action = 'insert' -- Only consider inserts
    AND str.metadata$isupdate = 'true' -- Ensure it's an update
    AND (trg.email <> str.mail 
      OR trg.phone_number <> str.phone 
      OR trg.salary <> str.salary 
      OR trg.department <> str.dept 
      OR trg.work_location <> str.loc)
      
THEN UPDATE SET
        trg.email = str.mail,
        trg.phone_number = str.phone,
        trg.salary = str.salary,
        trg.department = str.dept,
        trg.work_location = str.loc,
        trg.insert_ts = CURRENT_TIMESTAMP

WHEN NOT MATCHED 
THEN 
   INSERT (emp_id, emp_name, date_of_birth, email, phone_number, salary, department, work_location, insert_ts, lat_update_ts)
VALUES (str.eid, str.ename, str.dob, str.mail, str.phone, str.salary, str.dept, str.loc, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Create a scheduled task to automate the merge every 2 minutes
CREATE OR REPLACE TASK target.empl_scd1
  SCHEDULE = '2 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('staging.stream_stg_empl')
AS
  MERGE INTO target.empl_scd1 trg
    USING staging.stream_stg_empl str
    ON trg.emp_id = str.eid

WHEN MATCHED 
    AND str.metadata$action = 'insert'
    AND str.metadata$isupdate = 'true'
    AND trg.email <> str.mail 
        OR trg.phone_number <> str.phone 
         OR trg.salary <> str.salary 
         OR trg.department <> str.dept 
         OR trg.work_location <> str.loc
THEN 
    UPDATE SET
        trg.email = str.mail,
        trg.phone_number = str.phone,
        trg.salary = str.salary,
        trg.department = str.dept,
        trg.work_location = str.loc,
        trg.insert_ts = CURRENT_TIMESTAMP

WHEN NOT MATCHED 
THEN 
   INSERT (emp_id, emp_name, date_of_birth, email, phone_number, salary, department, work_location, insert_ts, lat_update_ts)
VALUES (str.eid, str.ename, str.dob, str.mail, str.phone, str.salary, str.dept, str.loc, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Resume the task to start automated execution
ALTER TASK target.empl_scd1 RESUME;

-- Show all tasks to check their status
SHOW TASKS;

-- Insert sample data into staging table
INSERT INTO STAGING.STG_EMPL 
VALUES
    (1, 'Rahul Sharma', '1986-04-15', 'rahul.sharma@gmail.com', '9988776655', 92000, 'Administration', 'Bangalore'),
    (2, 'Renuka Devi', '1993-10-19', 'renuka1993@yahoo.com', '+91 9911882255', 61000, 'Sales', 'Hyderabad'),
    (3, 'Kamalesh', '1991-02-08', 'kamal91@outlook.com', '9182736450', 59000, 'Sales', 'Chennai'),
    (4, 'Arun Kumar', '1989-05-20', 'arun_kumar@gmail.com', '901-287-3465', 74500, 'IT', 'Bangalore');

-- Verify data in staging table
SELECT * FROM staging.stg_empl;

-- Observe the streams now with change capture
SELECT * FROM staging.stream_stg_empl;

--After 1st run
-- Verify data in target table
SELECT * FROM target.empl_scd1;

-- Manually execute the task to apply changes
EXECUTE TASK target.empl_scd1;

-- Observe the streams now after consuming the changes
SELECT * FROM staging.stream_stg_empl;

---------------------------------------------------------

-- Make changes to the stage table (Assume it is truncate and load with new add updated records)
INSERT INTO STAGING.STG_EMPL 
VALUES
    (5, 'Arjun', '1989-05-20', 'arjun_kumar@gmail.com', '9703408236', 74000, 'IT', 'Rajoli');

-- Update an existing employee record to test update logic
UPDATE STAGING.STG_EMPL SET salary = 2000 , phone = '9911882255' WHERE eid = 2;

-- Observe the streams now with change capture
SELECT * FROM staging.stream_stg_empl;

--After 2 minutes (task will be running for every 2 mints)
--Verify the data in target table
SELECT * FROM target.empl_scd1;

-- Observe the streams now after consuming the changes
SELECT * FROM staging.stream_stg_empl;

--------------------------------------------------------------------

--One more time make changes to the stage table (Assume it is truncate and load with new add updated records)

INSERT INTO STAGING.STG_EMPL 
VALUES
    (6, 'kiran', '1989-05-20', 'kiran_kumar@gmail.com', '8247078947', 57000, 'Administration', 'ieeja');

update STAGING.STG_EMPL set phone = '9911882255', dept = 'it' where eid = 2;
update  STAGING.STG_EMPL set phone = '9012873465', dept = 'Administration' where eid = 4;

-- Observe the streams now with change capture
SELECT * FROM staging.stream_stg_empl;

--After 2 minutes (task will be running for every 2 mints)
--Verify the data in target table
SELECT * FROM target.empl_scd1;

-- Observe the streams now after consuming the changes
SELECT * FROM staging.stream_stg_empl;

---------------------
--Stop or drop the task, otherwise all your free credits will be consumed
ALTER TASK target.empl_scd1 SUSPEND;
