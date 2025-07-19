-----------------------------
SCD type-2 SP WITH MERGE 
-----------------------------

CREATE OR REPLACE TABLE staging_product (
    ProductID STRING,
    ProductName STRING,
    ProductCategory STRING,
    LastUpdated TIMESTAMP
);

INSERT INTO staging_product (ProductID, ProductName, ProductCategory, LastUpdated) VALUES
('P001', 'Laptop', 'Electronics', '2023-07-01 12:00:00'),
('P002', 'Smartphone', 'Electronics', '2023-07-01 12:00:00'),
('P003', 'Tablet', 'Electronics', '2023-07-01 12:00:00');



CREATE OR REPLACE TABLE dim_product (
    SurrogateKey NUMBER AUTOINCREMENT,
    ProductID STRING,
    ProductName STRING,
    ProductCategory STRING,
    ValidFrom TIMESTAMP,
    ValidTo TIMESTAMP,
    ActiveFlag BOOLEAN,
    PRIMARY KEY (SurrogateKey)
);

INSERT INTO dim_product (ProductID, ProductName, ProductCategory, ValidFrom, ValidTo, ActiveFlag) VALUES
('P001', 'Laptop', 'Electronics', '2023-07-01 12:00:00', '9999-12-31 23:59:59', TRUE),
('P002', 'Smartphone', 'Electronics', '2023-07-01 12:00:00', '9999-12-31 23:59:59', TRUE),
('P003', 'Tablet', 'Electronics', '2023-07-01 12:00:00', '9999-12-31 23:59:59', TRUE);


CREATE OR REPLACE PROCEDURE UPDATE_DIM_PRODUCT()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Expire old records where changes are detected
    MERGE INTO dim_product AS TARGET
    USING staging_product AS SOURCE
    ON SOURCE.ProductID = TARGET.ProductID AND TARGET.ActiveFlag = TRUE
    WHEN MATCHED AND (
        SOURCE.ProductName <> TARGET.ProductName OR
        SOURCE.ProductCategory <> TARGET.ProductCategory
    )
    THEN UPDATE SET 
        TARGET.ValidTo = SOURCE.LastUpdated,
        TARGET.ActiveFlag = FALSE;

    -- Step 2: Insert new records (new or changed data)
    INSERT INTO dim_product (
        ProductID, 
        ProductName, 
        ProductCategory, 
        ValidFrom, 
        ValidTo, 
        ActiveFlag
    )
    SELECT 
        SOURCE.ProductID,
        SOURCE.ProductName,
        SOURCE.ProductCategory,
        SOURCE.LastUpdated,
        '9999-12-31 23:59:59',
        TRUE
    FROM staging_product AS SOURCE
    LEFT JOIN dim_product AS TARGET
        ON SOURCE.ProductID = TARGET.ProductID 
           AND TARGET.ActiveFlag = TRUE
    WHERE TARGET.SurrogateKey IS NULL 
       OR SOURCE.ProductName <> TARGET.ProductName 
       OR SOURCE.ProductCategory <> TARGET.ProductCategory;

    RETURN 'SCD TYPE 2 DIMENSION TABLE IS UPDATED';
END;
$$;


CALL UPDATE_DIM_PRODUCT();

SELECT * FROM STAGING_PRODUCT;
SELECT * FROM DIM_PRODUCT;

INSERT INTO staging_product (ProductID, ProductName, ProductCategory, LastUpdated) VALUES
('P005', 'Gaming Laptop', 'Electronics', current_timestamp),
('P006', 'Smartphone', 'Mobile', current_timestamp);

UPDATE STAGING_PRODUCT SET PRODUCTnAME='max', LastUpdated=current_timestamp WHERE PRODUCTID='P003';
