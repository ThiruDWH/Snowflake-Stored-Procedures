--------------
SCD TYPE - 1
--------------

CREATE TABLE source_table (
    customer_id INT,
    name VARCHAR(100),
    address VARCHAR(255),
    phone VARCHAR(20),
    email VARCHAR(100),
    status VARCHAR(20)
);

INSERT INTO source_table (customer_id, name, address, phone, email, status) VALUES
(1, 'John Doe', '123 Elm Street, Springfield', '123-456-7890', 'john.doe@example.com', 'Active'),
(2, 'Jane Smith', '456 Oak Avenue, Metropolis', '987-654-3210', 'jane.smith@example.com', 'Inactive'),
(3, 'Alice Johnson', '789 Pine Road, Gotham', '555-123-4567', 'alice.j@example.com', 'Active'),
(4, 'Bob Brown', '321 Maple Lane, Star City', '444-555-6666', 'bob.brown@example.com', 'Pending');



CREATE TABLE target_dim (
    customer_id INT,
    name VARCHAR(100),
    address VARCHAR(255),
    phone VARCHAR(20),
    email VARCHAR(100),
    status VARCHAR(20)
);

INSERT INTO target_dim (customer_id, name, address, phone, email, status) VALUES
(1, 'John Doe', '123 Elm Street, Springfield', '123-456-7890', 'john.doe@example.com', 'Active'),
(2, 'Jane Smith', '999 Birch Boulevard, Metropolis', '987-654-3210', 'jane.smith@example.com', 'Inactive'), -- Address changed
(3, 'Alice Johnson', '789 Pine Road, Gotham', '555-123-4567', 'alice.j@example.com', 'Active'),
(4, 'Bob Brown', '321 Maple Lane, Star City', '777-888-9999', 'bob.brown@example.com', 'Inactive'); -- Phone changed

delete from target_dim;
select * from source_table;
select * from target_dim;--999 Birch Boulevard, Metropolis, 777-888-9999

MERGE INTO TARGET_DIM AS TGT
USING SOURCE_TABLE AS SCR
ON TGT.CUSTOMER_ID = SCR.CUSTOMER_ID

WHEN MATCHED AND 
      (TGT.CUSTOMER_ID <> SCR.CUSTOMER_ID OR
       TGT.NAME <> SCR.NAME OR
       TGT.ADDRESS <> SCR.ADDRESS OR
       TGT.PHONE <> SCR.PHONE OR
       TGT.EMAIL <> SCR.EMAIL OR
       TGT.STATUS <> SCR.STATUS)
    THEN 
  UPDATE  SET  TGT.CUSTOMER_ID = SCR.CUSTOMER_ID,
               TGT.NAME = SCR.NAME,
               TGT.ADDRESS = SCR.ADDRESS,
               TGT.PHONE = SCR.PHONE,
               TGT.EMAIL = SCR.EMAIL,
               TGT.STATUS = SCR.STATUS
WHEN NOT MATCHED THEN
INSERT (CUSTOMER_ID,NAME,ADDRESS,PHONE,EMAIL,STATUS) VALUES (SCR.CUSTOMER_ID,SCR.NAME,SCR.ADDRESS,SCR.PHONE,SCR.EMAIL,SCR.STATUS);

--01bbd48a-3201-8dbf-000c-3fa20010c67e
create table target_dim1 as
select * from target_dim before(statement => '01bbd48a-3201-8dbf-000c-3fa20010c67e');

select * from source_table;
select * from target_dim;
select * from target_dim1;

------------------------------
STORED_PROCEDURE
------------------------------
CREATE OR REPLACE PROCEDURE sp_merge_and_backup(do_rollback BOOLEAN)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rollback_flag BOOLEAN;
BEGIN
    -- Assign input to local variable
    rollback_flag := do_rollback;

    -- Step 1: Backup current data
    CREATE OR REPLACE TEMP TABLE TARGET_DIM_BACKUP AS
    SELECT * FROM TARGET_DIM;

    -- Step 2: Perform the MERGE
    MERGE INTO TARGET_DIM
    USING SOURCE_TABLE
    ON TARGET_DIM.CUSTOMER_ID = SOURCE_TABLE.CUSTOMER_ID

    WHEN MATCHED AND 
         (TARGET_DIM.NAME <> SOURCE_TABLE.NAME OR
          TARGET_DIM.ADDRESS <> SOURCE_TABLE.ADDRESS OR
          TARGET_DIM.PHONE <> SOURCE_TABLE.PHONE OR
          TARGET_DIM.EMAIL <> SOURCE_TABLE.EMAIL OR
          TARGET_DIM.STATUS <> SOURCE_TABLE.STATUS)
    THEN UPDATE SET 
          NAME = SOURCE_TABLE.NAME,
          ADDRESS = SOURCE_TABLE.ADDRESS,
          PHONE = SOURCE_TABLE.PHONE,
          EMAIL = SOURCE_TABLE.EMAIL,
          STATUS = SOURCE_TABLE.STATUS

    WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, NAME, ADDRESS, PHONE, EMAIL, STATUS)
    VALUES (SOURCE_TABLE.CUSTOMER_ID, SOURCE_TABLE.NAME, SOURCE_TABLE.ADDRESS, SOURCE_TABLE.PHONE, SOURCE_TABLE.EMAIL, SOURCE_TABLE.STATUS);

    -- Step 3: Rollback if requested
    IF (rollback_flag) THEN
        DELETE FROM TARGET_DIM;
        INSERT INTO TARGET_DIM
        SELECT * FROM TARGET_DIM_BACKUP;

        RETURN 'Merge reverted. Backup restored.';
    ELSE
        RETURN 'Merge completed successfully.';
    END IF;

END;
$$;


CALL sp_merge_and_backup(TRUE);   -- Rollback after merge
CALL sp_merge_and_backup(FALSE);  -- Apply merge only

select * from target_dim;
select * from target_dim1;

select * from TARGET_DIM_BACKUP;
