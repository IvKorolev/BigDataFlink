DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT,
    pet_category TEXT
);

CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(10,2),
    product_quantity INTEGER,
    product_weight NUMERIC(10,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE dim_seller (
    seller_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE dim_store (
    store_id INTEGER PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE dim_supplier (
    supplier_id INTEGER PRIMARY KEY,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    quarter INTEGER
);

CREATE TABLE fact_sales (
    sale_id INTEGER PRIMARY KEY,
    date_id INTEGER,
    customer_id INTEGER,
    seller_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    supplier_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(10,2)
);