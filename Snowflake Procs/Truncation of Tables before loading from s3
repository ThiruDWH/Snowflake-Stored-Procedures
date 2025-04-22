--------------------------------------------
Truncation of Tables before loading from s3
--------------------------------------------
CREATE OR REPLACE PROCEDURE TRUNCATE_TABLES_FROM_SCHEMA()
RETURNS STRING
LANGUAGE SQL
AS
$$

   DECLARE
         sql_statement  VARCHAR;
         table_name      VARCHAR;
    
        table_cursor CURSOR
        FOR
       SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_CATALOG = 'DUMMY_DB'
        AND  TABLE_SCHEMA  = 'RAW_SCHEMA';

   BEGIN
     open table_cursor;
     FOR table_rec IN table_cursor
     DO
      table_name := table_rec.TABLE_NAME;
      sql_statement := 'TRUNCATE TABLE '||table_name;
      EXECUTE IMMEDIATE sql_statement; 
     END FOR;
     RETURN 'Schema Tables are Truncated Successfully.';
   END;

$$;

CALL TRUNCATE_TABLES_FROM_SCHEMA();

SELECT TABLE_NAME, ROW_COUNT FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_CATALOG = 'DUMMY_DB'
 AND  TABLE_SCHEMA  = 'RAW_SCHEMA';
