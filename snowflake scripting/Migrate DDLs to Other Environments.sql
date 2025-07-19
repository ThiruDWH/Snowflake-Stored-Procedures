--CREATE DATABASES AND SCHEMAS FOR 3 ENVIRONMENTS
CREATE DATABASE IF NOT EXISTS DEV_EMP;
CREATE SCHEMA IF NOT EXISTS DEV_EMP.HRDATA; 
CREATE SCHEMA IF NOT EXISTS DEV_EMP.WORK;
CREATE SCHEMA IF NOT EXISTS DEV_EMP.PROCS;

CREATE DATABASE IF NOT EXISTS TEST_EMP;
CREATE SCHEMA IF NOT EXISTS TEST_EMP.HRDATA;
CREATE SCHEMA IF NOT EXISTS TEST_EMP.WORK;
CREATE SCHEMA IF NOT EXISTS TEST_EMP.PROCS;

CREATE DATABASE IF NOT EXISTS PROD_EMP;
CREATE SCHEMA IF NOT EXISTS PROD_EMP.HRDATA;
CREATE SCHEMA IF NOT EXISTS PROD_EMP.WORK;
CREATE SCHEMA IF NOT EXISTS PROD_EMP.PROCS;

--Create a work table in all environments
CREATE TABLE DEV_EMP.WORK.MIGRATE_TABLE(TABLE_NAME VARCHAR(50));
CREATE TABLE TEST_EMP.WORK.MIGRATE_TABLE(TABLE_NAME VARCHAR(50));
CREATE TABLE PROD_EMP.WORK.MIGRATE_TABLE(TABLE_NAME VARCHAR(50));

--MIGRATING TABLE DDL's FROM ONE DATABASE TO ANOTHER DATABASE
CREATE OR REPLACE PROCEDURE TEST_EMP.PROCS.SP_MIGRATE_TABLE_DDLS("SRCDB" VARCHAR, "SRCSCHEMA" VARCHAR, "TGTDB" VARCHAR, "TGTSCHEMA" VARCHAR, "ALL_FLAG" VARCHAR(1), "REPLACE_FLAG" VARCHAR(1))
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS

DECLARE

--ALL_FLAG - 'Y' if you want to migrate all tables from the schema, 'N' if you want to migrate specific tables
--REPLACE_FLAG - 'Y' if you want to replace if the table already exists, 'N' if you want to skip

cur_all_tbl cursor for SELECT DISTINCT TABLE_NAME FROM TABLE(?) WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE';
cur_some_tbl cursor for SELECT DISTINCT TABLE_NAME FROM TABLE(?);

SRCTBL VARCHAR;
ddl_statement VARCHAR;

BEGIN

IF (:ALL_FLAG = 'Y') THEN

    OPEN cur_all_tbl USING(:SRCDB||'.INFORMATION_SCHEMA.TABLES', :SRCSCHEMA);

    for rec in cur_all_tbl do

        SRCTBL := rec.TABLE_NAME;
        SELECT get_ddl('TABLE', :SRCDB||'.'||:SRCSCHEMA||'.'||:SRCTBL) into :ddl_statement;

        IF(:REPLACE_FLAG = 'Y') THEN
            ddl_statement := REPLACE(:ddl_statement, 'create or replace TABLE ',
            'create or replace TABLE '||:TGTDB||'.'||:TGTSCHEMA||'.');
        ELSE
            ddl_statement := REPLACE(:ddl_statement, 'create or replace TABLE ', 
            'create table if not exists '||:TGTDB||'.'||:TGTSCHEMA||'.');
        END IF;

        execute immediate :ddl_statement;

    end for;
    CLOSE cur_all_tbl;

ELSE
    OPEN cur_some_tbl USING(:TGTDB||'.WORK.MIGRATE_TABLES');

    for rec in cur_some_tbl do

        SRCTBL := rec.TABLE_NAME;
        SELECT get_ddl('TABLE', :SRCDB||'.'||:SRCSCHEMA||'.'||:SRCTBL) into :ddl_statement;

        IF(:REPLACE_FLAG = 'Y') THEN
            ddl_statement := REPLACE(:ddl_statement, 'create or replace TABLE ', 
            'create or replace table '||:TGTDB||'.'||:TGTSCHEMA||'.');

        ELSE 
            ddl_statement := REPLACE(:ddl_statement, 'create or replace TABLE ', 
            'create table if not exists '||:TGTDB||'.'||:TGTSCHEMA||'.'); 
        END IF;

        execute immediate :ddl_statement;

    end for;
    CLOSE cur_some_tbl;

END IF;

RETURN 'Tables Migrated Successfully';

END;

--INSERT INTO TEST_EMP.WORK.MIGRATE_TABLES VALUES ('EMPLOYEES'), ('DEPARTMENTS');
--CALL TEST_EMP.PROCS.SP_MIGRATE_TABLE_DDLS('DEV_EMP', 'HRDATA', 'TEST_EMP', 'HRDATA', 'N', 'Y');
--CALL TEST_EMP.PROCS.SP_MIGRATE_TABLE_DDLS('DEV_EMP', 'HRDATA', 'TEST_EMP', 'HRDATA', 'Y', 'Y');


--MIGRATING VIEWS  FROM ONE DATABASE TO ANOTHER DATABASE
CREATE OR REPLACE PROCEDURE TEST_EMP.PROCS.SP_MIGRATE_VIEWS("SRCDB" VARCHAR, "SRCSCHEMA" VARCHAR, "TGTDB" VARCHAR, "TGTSCHEMA" VARCHAR, "ALL_FLAG" VARCHAR(1))
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS

DECLARE

--ALL_FLAG - 'Y' if you want to migrate all views from the schema, 'N' if you want to migrate specific viwes

cur_all_views cursor for SELECT DISTINCT TABLE_NAME as VIEW_NAME FROM TABLE(?) WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'VIEW';
cur_some_views cursor for SELECT DISTINCT VIEW_NAME FROM TABLE(?);
cur_env cursor for SELECT SRC_ENV_DB, TGT_ENV_DB FROM TABLE(?);

SRCVIEW VARCHAR;
ddl_statement VARCHAR;

BEGIN

IF (:ALL_FLAG = 'Y') THEN

    OPEN cur_all_views USING(:SRCDB||'.INFORMATION_SCHEMA.TABLES', :SRCSCHEMA);

    for rec in cur_all_views do

        SRCVIEW := rec.VIEW_NAME;
        SELECT get_ddl('VIEW', :SRCDB||'.'||:SRCSCHEMA||'.'||:SRCVIEW) into :ddl_statement;
        
        --replace the database in the view definition
        OPEN cur_env USING(:TGTDB||'.WORK.XW_ENV_DATABASES');
        for rec1 in cur_env do 
            ddl_statement := REGEXP_REPLACE(:ddl_statement, rec1.SRC_ENV_DB, rec1.TGT_ENV_DB, 1, 0, 'i');
        end for
         CLOSE cur_env;

         ddl_statement := REGEXP_REPLACE(:ddl_statement, 'create or replace view ', 
         'create or replace view '||:TGTDB||'.'||:TGTSCHEMA||'.', 1, 0, 'i');

         execute immediate :ddl_statement;

    end for;
    CLOSE cur_all_views;
        
ELSE
    OPEN cur_some_views USING(:TGTDB||'.WORK.MIGRATE_VIEWS');

    for rec in cur_some_views do

        SRCVIEW := rec.VIEW_NAME;
        SELECT get_ddl('VIEW', :SRCDB||'.'||:SRCSCHEMA||'.'||:SRCVIEW) into :ddl_statement;

        OPEN cur_env USING(:TGTDB||'.WORK.XW_ENV_DATABASES');
        for rec1 in cur_env do 
            ddl_statement := REGEXP_REPLACE(:ddl_statement, rec1.SRC_ENV_DB, rec1.TGT_ENV_DB, 1, 0, 'i');
        end for
         CLOSE cur_env;

         ddl_statement := REGEXP_REPLACE(:ddl_statement, 'create or replace view ', 
         'create or replace view '||:TGTDB||'.'||:TGTSCHEMA||'.', 1, 0, 'i');

         execute immediate :ddl_statement;

    end for;
    CLOSE cur_some_views;

END IF;

RETURN 'Views Migrated Successfully';

END;

--INSERT INTO TEST_EMP.WORK.MIGRATE_VIEWA VALUES ('V_COUNTRIES'), ('V_DEPT_WISE_EMP_HIRED'), ('V_EMPLOYEES_DETAILS');
--CALL TEST_EMP.PROCS.SP_MIGRATE_VIEWS('DEV_EMP', 'HRDATA', 'TEST_EMP', 'HRDATA', 'N');
--CALL TEST_EMP.PROCS.SP_MIGRATE_VIEWS('DEV_EMP', 'HRDATA', 'TEST_EMP', 'HRDATA', 'Y');

