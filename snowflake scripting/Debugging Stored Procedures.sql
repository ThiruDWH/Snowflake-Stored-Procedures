--find given number is even or odd (i missed this --end if;)
 set N = 1237

  execute immediate
  $$
    begin
      IF($N % 2 = 0) then
          return 'given number is EVEN';
      ELSE
         return 'given number is ODD';
      
    end;
  $$
;

--find prime numbers upto given number
create or replace procedure emp.procs.sp_while_prime_number("N" integer)
returns varchar
language sql
execute as caller
as
   DECLARE 
     i INTEGER default '2 ';
     j INTEGER;
     flag INTEGER;
     prime VARCHAR default '2 ';
   BEGIN
      WHILE (i <= N)
      DO
          flag := 0;
          FOR j IN 2 to i-1
          DO
             IF (i % j = 0) THEN
                flag := i;
                break;
             END IF;
          END FOR;
          IF (flag = 0) THEN
              prime := prime || ', ' || i;
          END IF;
     i =: i+1;    -- i := i+1;
     END WHILE;
     
  RETURN prime;
  END;

call emp.procs.sp_while_prime_numbers(100);
