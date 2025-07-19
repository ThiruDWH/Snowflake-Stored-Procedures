CREATE DATABASE IF NOT EXISTS EMP;
CREATE SCHEMA IF NOT EXISTS EMP.UDFs;

--Create a function to convert Julian date format to Gregorian date format
CREATE OR REPLACE FUNCTION EMP.UDFs.UDF_JUL_TO_GRE_DATE("JDATE" VARCHAR)
RETURNS DATE
LANGUAGE SQL
AS

$$

SELECT
TO_DATE(DATEADD(DAYS,
                cast(SUBSTR(JDATE,length(JDATE)-2,3) as number) -1 ,
                DATEADD(YEAR, cast(SUBSTR(JDATE,1,length(JDATE)-3) as number), '1900-01-01')
              )
        ) as GREG_DATE
$$;

SELECT 
EMP.UDFs.UDF_JUL_TO_GRE_DATE('111152'),
EMP.UDFs.UDF_JUL_TO_GRE_DATE('098032'),
EMP.UDFs.UDF_JUL_TO_GRE_DATE('125232');
-----------------

--If we receive Jul date as 5 digits number like 98032, then pad one 0 at left side --098032
--LPAD(JDATE,6,0)

--Create a function to convert Julian date format to Gregorian date format
CREATE OR REPLACE FUNCTION EMP.UDFs.UDF_JUL_TO_GRE_DATE("JDATE" VARCHAR)
RETURNS DATE
LANGUAGE SQL
AS

$$

WITH JUL_DATE as (SELECT LPAD(JDATE,6,0) as JD)

SELECT
TO_DATE(DATEADD(DAYS,
                cast(SUBSTR(JD,length(JD)-2,3) as number) -1 ,
                DATEADD(YEAR, cast(SUBSTR(JD,1,length(JD)-3) as number), '1900-01-01')
              )
        ) as GREG_DATE
FROM JUL_DATE        
$$;

SELECT 
 EMP.UDFs.UDF_JUL_TO_GRE_DATE('111152'),
 EMP.UDFs.UDF_JUL_TO_GRE_DATE('098032'),
 EMP.UDFs.UDF_JUL_TO_GRE_DATE('98032'),
 EMP.UDFs.UDF_JUL_TO_GRE_DATE('98033');

