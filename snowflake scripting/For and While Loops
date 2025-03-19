--code to print stars in triangle shape
execute immediate
 $$
   DECLARE 
       i INTEGER;
       j INTEGER;
       pattern VARCHAR default '';
   BEGIN 
      FOR i in 1 to 5
      DO 
        FOR j in 1 to i
          DO
            pattern := pattern || '*\t';
          END FOR;
            pattern := pattern || '\n';
       END FOR;
    RETURN pattern;
  END;
$$                                             

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
     i := i+1;
     END WHILE;
     
  RETURN prime;
  END;

call emp.procs.sp_while_prime_numbers(100);
call emp.procs.so_while_prime_number(4583456437865); --33 minutes still running 

--find prime numbers upto given number in the form of a table
create or replace procedure emp.procs.sp_while_prime_numbers("N" INTEGER)
returns table(prime integer)
language sql
execute as caller
as 
  DECLARE 
    i INTEGER default 2;
    j INTEGER;
    flag INTEGER;
    res RESULTSET;
    prime VARCHAR;
  BEGIN
     create or replace temporary table prime_numbers(prime integer);
     WHILE (i <= N)
     DO
         flag := 0;
         FOR j IN 2 TO i-1
         DO
             IF (i % j = 0 and i <> 2) THEN
                flag := 1;
                break;
             END IF;
         END FOR;
         IF (flag = 0) THEN
            --prime := prime || ', ' || i;
            insert into prime_numbers values (:i);
         END IF;
    i := i+1;
    END WHILE;

    prime := 'SELECT * FROM PRIME_NUMBERS';
    res := (EXECUTE IMMEDIATE :prime);

  RETURN TABLE(res);
  END;

call emp.procs.sp_while_prime_numbers(100);
  
  
        
