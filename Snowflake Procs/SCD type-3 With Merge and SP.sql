-- Create the staging table
CREATE OR REPLACE TABLE staging_table (
    ProductID INT,
    ProductName STRING,
    ProductCategory STRING,
    ProductDescription STRING,
    EffectiveDate DATE
);

-- Create the dimension table
CREATE OR REPLACE TABLE dimension_table (
    SurrogateKey INT AUTOINCREMENT,
    ProductID INT,
    ProductName STRING,
    PreviousProductName STRING,
    ProductCategory STRING,
    PreviousProductCategory STRING,
    ProductDescription STRING,
    EffectiveDate DATE,
    CurrentFlag BOOLEAN
);

-- Insert statement for the staging table
INSERT INTO staging_table (ProductID, ProductName, ProductCategory, ProductDescription, EffectiveDate)
VALUES
    (1, 'Product A', 'Category X', 'Description for Product A', '2024-01-01'),
    (2, 'Product B', 'Category Y', 'Description for Product B', '2024-01-01');


select * from staging_table;
select * from dimension_table;


UPDATE staging_table 
SET ProductName = 'Product T', ProductCategory = 'Category Z' 
WHERE ProductID = 1;


MERGE INTO DIMENSION_TABLE AS TGT
USING staging_table AS SRC
ON TGT.PRODUCTID = SRC.PRODUCTID
WHEN MATCHED AND TGT.CURRENTFLAG = TRUE THEN
UPDATE SET TGT.CURRENTFLAG = FALSE,
           TGT.EFFECTIVEDATE = SRC.EffectiveDate
WHEN MATCHED AND TGT.CURRENTFLAG = FALSE THEN
UPDATE SET TGT.ProductName = SRC.PRODUCTNAME,
           TGT.PreviousProductName = TGT.ProductName,
           TGT.ProductCategory = SRC.ProductCategory,
           TGT.PreviousProductCategory = TGT.ProductCategory,
           TGT.ProductDescription = SRC.ProductDescription,
           TGT.EffectiveDate = SRC.EffectiveDate,
           TGT.CurrentFlag = TRUE
WHEN NOT MATCHED THEN
INSERT (ProductID, ProductName, PreviousProductName, ProductCategory, PreviousProductCategory, ProductDescription, EffectiveDate, CurrentFlag)
VALUES (SRC.ProductID, SRC.ProductName, NULL, SRC.ProductCategory, NULL, SRC.ProductDescription, SRC.EffectiveDate, TRUE);


-------------------------
SCD TYPE 3 MERGE WITH SP
-------------------------

CREATE OR REPLACE PROCEDURE UPDATE_DIMENSION_TABLE_SCD3()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

MERGE INTO DIMENSION_TABLE AS TGT
USING staging_table AS SRC
ON TGT.PRODUCTID = SRC.PRODUCTID

WHEN MATCHED AND TGT.CURRENTFLAG = TRUE THEN
UPDATE SET TGT.CURRENTFLAG = FALSE,
           TGT.EFFECTIVEDATE = SRC.EffectiveDate

WHEN MATCHED AND TGT.CURRENTFLAG = FALSE THEN
UPDATE SET TGT.ProductName = SRC.PRODUCTNAME,
           TGT.PreviousProductName = TGT.ProductName,
           TGT.ProductCategory = SRC.ProductCategory,
           TGT.PreviousProductCategory = TGT.ProductCategory,
           TGT.ProductDescription = SRC.ProductDescription,
           TGT.EffectiveDate = SRC.EffectiveDate,
           TGT.CurrentFlag = TRUE

WHEN NOT MATCHED THEN
INSERT (ProductID, ProductName, PreviousProductName, ProductCategory, PreviousProductCategory, ProductDescription, EffectiveDate, CurrentFlag)
VALUES (SRC.ProductID, SRC.ProductName, NULL, SRC.ProductCategory, NULL, SRC.ProductDescription, SRC.EffectiveDate, TRUE);

RETURN 'SCD TYPE 3 DIMENSION TABLE IS UPDATED';

END;
$$;

-- First run: Inserts fresh records into dimension table
CALL UPDATE_DIMENSION_TABLE_SCD3();


UPDATE staging_table 
SET ProductName = 'Product T', ProductCategory = 'Category Z' 
WHERE ProductID = 1;

select * from staging_table;
select * from dimension_table;
