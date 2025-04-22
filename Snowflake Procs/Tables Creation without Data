select * from information_schema.tables
where table_catalog = 'MYDB'
and   table_schema  = 'MY_SC'
and   table_type    ='BASE TABLE';

create table test like target_dim; --Create table without data
select * from test;
drop table test;

CREATE TABLE NEW_DEV_DB.NEW_DEV_SC.TABLE1 LIKE PROD_DB.PROD_SC.TABLE2;

SELECT TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'MYDB'
AND TABLE_SCHEMA    = 'MY_SC'
AND TABLE_TYPE      = 'BASE TABLE';     

CREATE DATABASE NEW_DEV_DB;
CREATE SCHEMA NEW_DEV_SC;

SHOW TABLES;

-----------------------------
Tables Creation without Data
-----------------------------

CREATE OR REPLACE PROCEDURE TABLE_CREATION_PROD()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    table_list_cursor CURSOR FOR
        SELECT 
            TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS SRC_TABLE,
            TABLE_NAME AS TRG_TABLE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = 'MYDB'
          AND TABLE_SCHEMA = 'MY_SC'
          AND TABLE_TYPE = 'BASE TABLE';

    sql_statement VARCHAR;

BEGIN
    OPEN table_list_cursor;

    FOR tb IN table_list_cursor DO
        sql_statement := 
            'CREATE OR REPLACE TABLE NEW_DEV_DB.NEW_DEV_SC.' || tb.TRG_TABLE ||
            ' LIKE ' || tb.SRC_TABLE;
        EXECUTE IMMEDIATE sql_statement;
    END FOR;

    RETURN 'Tables Created Successfully Without Data.';
END;
$$;

CALL TABLE_CREATION_PROD();
--------
SHOW TABLES;
