--find given number is even or odd
 set N = 1237

  execute immediate
  $$
    begin
      IF($N % 2 = 0) then
          return 'given number is EVEN';
      ELSE
         return 'given number is ODD';
      end if;
    end;
  $$
;

--find the tax amount for given monthly salary amount
set monthly_salary = 90000;

execute immediate
 $$
   DECLARE
       tax float;
       tax10 float; tax20 float; tax30 float;

   BEGIN
       IF($monthly_salary * 12 <= 500000) then
         tax := 0;
       ELSEIF(($monthly_salary * 12) between 500001 and 1000000) then
          tax := (($monthly_salary * 12) - 500000) * 10/100;
       ELSEIF(($monthly_salary * 12) between 1000001 and 1500000) then
          tax10 := (1000000 - 500000) * 10/100;
          tax20 := (($monthly_salary * 12) - 1000000) * 20/100;
          tax := tax10 + tax20;
       ELSE
          tax10 := (1000000 - 500000) * 10/100;
          tax20 := (1500000 - 1000000) * 20/100;
          tax30 := (($monthly_salary * 12) - 1500000) * 30/100;
       END IF;
    return 'calculated annual tax is: ' || tax;
  end;
$$
;
   
    
--find the tax amount for given monthly salary amount
create or replace procedure emp.procs.sp_case_demo(mon_gros_sal float)
returns float
language sql
execute as caller
as
declare 
    tax float;
begin
    tax := case 
           when (mon_gros_sal * 12 <= 500000) then 0
           when ((mon_gros_sal * 12) between 500001 and 1000000)
                then ((mon_gros_sal * 12) - 500000) * 10/100
           when ((mon_gros_sal * 12) between 1000001 and 1500000)
                then ((1000000 - 500000) * 10/100) + (((mon_gros_sal * 12) - 1000000) * 20/100)
           else
                ((1000000 - 500000) * 10/100) + ((1500000 - 1000000) * 20/100) + (((mon_gros_sal * 12) - 1500000) * 30/100)
           end;
    return 'calculated annual tax is: ' || tax;
end;


call emp.procs.sp_case_demo(85000);
