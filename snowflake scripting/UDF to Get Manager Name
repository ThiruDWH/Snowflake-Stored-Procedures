CREATE DATABASE IF NOT EXISTS EMP;
CREATE SCHEMA IF NOT EXISTS EMP.UDFs;

--Create a function to get the manager names of all employees
CREATE OR REPLACE FUNCTION EMP.UDFs.UDF_MANAGER_DETAILS()
RETURNS TABLE(emo_name varchar, namager_name VARCHAR)
LANGUAGE SQL
AS

$$

SELECT 
   empl.first_name||' '||empl.last_name as emp_name,
   mgr.first_name||' '||mgr.last_name as manager_name
   FROM emp.hrdata.employees empl
   JOIN emp.hrdata.employees mgr
   ON empl.manager_id = mgr.employee_id

$$;

SELECT emp_name, manager_name FROM TABLE(EMP.UDFs.UDF_MANAGER_DETAILS());

--------------

--Create a function to get manager and his direct reporteess
CREATE OR REPLACE FUNCTION EMP.UDFs.UDF_DIRECT_REPORTEES()
RETURNS TABLE(manager_name VARCHAR, direct_reportees VARCHAR)
LANGUAGE SQL
AS

$$

SELECT manager_name, LISTAGG(emp_name, ', ') FROM
(    SELECT 
     empl.first_name||' '||empl.last_name as emp_name,
     mgr.first_name||' '||mgr.last_name as manager_name
     FROM emp.hrdata.employees empl
     JOIN emp.hrdata.employees mgr
     ON empl.manager_id = mgr.employee_id
) abc
GROUP BY manager_name

$$;  

SELECT manager_name, direct_reportees FROM TABLE(UDF_DIRECT_REPORTEES());
