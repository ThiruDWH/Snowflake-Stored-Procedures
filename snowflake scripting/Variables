-- Program 1: Concatenating First Name, Middle Name, and Last Name using EXECUTE IMMEDIATE  
EXECUTE IMMEDIATE
$$
    DECLARE 
      FIRST_NAME VARCHAR DEFAULT 'HI THIS IS';
      LAST_NAME VARCHAR;
      FULL_NAME STRING;
    BEGIN 
       LET MIDDLE_NAME := ' ';
       LAST_NAME := 'THIRUMALESH';
       FULL_NAME := FIRST_NAME || MIDDLE_NAME || LAST_NAME;
       RETURN FULL_NAME;
    END;
$$

-- Program 2: Calculating Profit by Subtracting Cost from Revenue  
EXECUTE IMMEDIATE
$$
    DECLARE
        PROFIT NUMBER (38,2) DEFAULT 0.0;
    BEGIN 
       LET COST NUMBER(38,0) :=100;
       LET REVENUE NUMBER(38,2) DEFAULT 130;
       PROFIT := REVENUE - COST;
       RETURN PROFIT;
    END;
$$

-- Program 3: Performing Arithmetic Operations Using Session Variables  
SET A = 60;
SET (B,C) = (220,10);
SET AGE = 'MY AGE IS ';

EXECUTE IMMEDIATE
$$
   DECLARE
      D INTEGER;
   BEGIN
     D := ($A + $B) / $C;
     RETURN $AGE || D;
   END;
$$

-- Program 4: Concatenating a String and Performing Addition with Session Variables  
SET (A,B,PLACE) = (30,52,'HYDERABAD');

EXECUTE IMMEDIATE
$$
   BEGIN
    RETURN $PLACE || ' - ' || ($A+$B);
   END;
$$

-- Program 5: Session Variable can't be modifed
SET A = 60;
EXECUTE IMMEDIATE
$$
    BEGIN
        A := 100;
        --$A := 100;
        RETURN $A;
    END;
$$
