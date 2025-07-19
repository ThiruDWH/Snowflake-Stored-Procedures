CREATE OR REPLACE PROCEDURE mydb.staging.sp_automate_data_copy()
RETURNS VARCHAR
LANGUAGE SQL 
EXECUTE AS CALLER
AS

DECLARE
curs cursor for SELECT * FROM mydb.control.copy_ctrl WHERE is_active = True;

tbl string;
sch string;
db string;
st_int string;
st_loc string;
file_typ string;
file_pat string;
fld_dlm string;
skp_hdr string;
forc string;
on_err string;
trun_col string;
cnt integer;
ret string;
file_format1 string;
copy_stmt string;
create_stage_stmt string;
fp string;


BEGIN

ret := '';

for rec in curs
do
   tbl := rec.stage_table_name;
   sch := rec.schema_name;
   db := rec.database_name;
   st_int := rec.storage_int;
   st_loc := rec.storage_loc;
   file_typ := rec.file_typ;
   file_pat := rec.files_pattern;
   fld_dlm := rec.field_delim;
   skp_hdr := rec.skip_header;
   forc := rec.force;
   on_err := rec.on_error;
   trun_col := rec.truncate_cols;

   if(:file_typ = 'csv') then
       file_format1 := '(type='||:file_typ||' skip_header='||:skp_hdr||'
       field_delimiter=\''||:fld_dlm||'\' empty_field_as_null = TRUE)';
   else 
       file_format1 := '(type='||:file_typ||')';
   end if;

   create_stage_stmt := 'CREATE OR REPLACE TEMPORARY STAGE
   mydb.external_stages.s3_stage
   URL = \''|| :st_loc || '\'
   STORAGE_INTEGRATION = '|| :st_int ||'
   FILE_FORMAT = ' ||:file_format1 ;

   execute immediate create_stage_stmt;

   fp := substring(:file_pat, 3, length(:file_pat)-4);

   list @mydb.external_stage.s3_satge;

   SELECT COUNT(1) INTO cnt FROM table(result_scan(last_query_id()));

   if (:cnt > 0) then

      copy_stmt := 'COPY INTO '||:db||'.'||:sch||'.'||:tbl || '
      FROM @mydb.external_stage.s3_satge
      pattern = \'' || :file_pat || '\'
      ON_ERROR = ' || :on_err || '
      FORCE = ' || :forc || '
      TRUNCATECOLUMNS = ' || :trun_col
      ;

      execute immediate copy_stmt;

        ret := :ret || :fp || ' Format files completed successfully. \n';

   else
        ret := :ret || :fp || ' Format files not present in the external stage. \n';
   end if;

end for;

return :ret;

end;
---------------------------------------------------------

-- counts before executing the procedure
/*
select count(1) from mydb.staging.customer_data;
select count(1) from mydb.staging.pets_data_raw;  
select count(1) from mydb.staging.emp_data;  
select count(1) from mydb.staging.customer; 
*/

call mydb.staging.sp_automate_data_copy();

-- counts after executing the procedure
/*
select count(1) from mydb.staging.customer_data;
select count(1) from mydb.staging.pets_data_raw;  
select count(1) from mydb.staging.emp_data;  
select count(1) from mydb.staging.customer; 
*/
-----------------------------------------------------------------------

--Automate this process to run every day
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = ' USING CRON 0 7 * * * UTC'
AS
call mydb.staging.sp_automate_data_copy();

--Process different files at different timings
CREATE OR REPLACE PROCEDURE mydb.staging.sp_automate_data_copy("table_name" varchar)

curs cursor for SELECT * FROM mydb.control.copy_ctrl WHERE is_active = true
and stage_table_name=:table_name;

--every day at 8am
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS 
call mydb.staging.sp_automate_data_copy(customer_data);

--every day at 10am and 10pm
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 0 10,22 * * * UTC'
AS
call mydb.staging.sp_automate_data_copy();

--every monday at 6:30am
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 30 6 * * 1 UTC'
AS
call mydb.staging.sp_automate_data_copy('pets_data_raw');
