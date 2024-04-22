--Changing Data Types of the columns of orders_table
ALTER TABLE orders_table
ALTER COLUMN date_uuid SET DATA TYPE UUID USING uuid::uuid,
ALTER COLUMN user_uuid SET DATA TYPE UUID USING uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

--Changing Data Types of the columns of dim_users
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(4),
ALTER COLUMN date_of_birth TYPE DATE USING date_added ::DATE;
ALTER COLUMN join_date TYPE DATE USING date_added ::DATE;
ALTER COLUMN user_uuid SET DATA TYPE UUID USING uuid::uuid;

--Changing Data Types of the columns of dim_products
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(18),
ALTER COLUMN product_code TYPE VARCHAR(12),
ALTER COLUMN weight_class TYPE VARCHAR(15),
ALTER COLUMN uuid SET DATA TYPE UUID USING uuid::uuid,
ALTER COLUMN date_added TYPE DATE USING date_added ::DATE;

--Changing Data Types of the columns of dim_date_times
ALTER TABLE dim_date_times
ALTER COLUMN "month" TYPE VARCHAR(2),
ALTER COLUMN "year" TYPE VARCHAR(4),
ALTER COLUMN "day" TYPE VARCHAR(2),
ALTER COLUMN "time_period" TYPE VARCHAR(10),
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::uuid;

--Changing Data Types of the columns of dim_store_details
ALTER TABLE dim_store_details
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE USING opening_date ::DATE;

--Changing Data Types of the columns of dim_card_details
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(25),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed ::DATE;

-- Checking if the columns data type have been updated
SELECT *
FROM 
    information_schema.columns 
WHERE 
    table_name = 'dim_card_details'
    AND table_schema = 'public';
	
--checking for and deleting the NULL rows from the table	dim_users
SELECT *
FROM 
    dim_users
WHERE user_uuid is NULL;

DELETE FROM dim_users WHERE user_uuid IS NULL;

-- Adding primary keys to all tables except orders_table 
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);


ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);


ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

-- checking changes have been made 
SELECT *
FROM 
    information_schema.columns 
WHERE 
    table_name = 'dim_users'
    AND table_schema = 'public';
	

