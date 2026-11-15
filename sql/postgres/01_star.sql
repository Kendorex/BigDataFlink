CREATE TABLE dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id TEXT NOT NULL UNIQUE,
    source_customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE dim_seller (
    seller_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id TEXT NOT NULL UNIQUE,
    source_seller_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id TEXT NOT NULL UNIQUE,
    source_product_id INTEGER,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12,2),
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
    product_expiry_date DATE,
    pet_category TEXT
);

CREATE TABLE dim_store (
    store_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    UNIQUE NULLS NOT DISTINCT (store_name, store_location, store_city, store_state,
                              store_country, store_phone, store_email)
);

CREATE TABLE dim_supplier (
    supplier_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT,
    UNIQUE NULLS NOT DISTINCT (supplier_name, supplier_contact, supplier_email,
                              supplier_phone, supplier_address, supplier_city, supplier_country)
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

CREATE TABLE fact_sales (
    sale_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id TEXT NOT NULL UNIQUE,
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL CHECK (source_row > 0),
    source_id INTEGER NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    seller_key BIGINT NOT NULL REFERENCES dim_seller(seller_key),
    product_key BIGINT NOT NULL REFERENCES dim_product(product_key),
    store_key BIGINT NOT NULL REFERENCES dim_store(store_key),
    supplier_key BIGINT NOT NULL REFERENCES dim_supplier(supplier_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    sale_quantity INTEGER NOT NULL CHECK (sale_quantity > 0),
    sale_total_price NUMERIC(12,2) NOT NULL CHECK (sale_total_price >= 0),
    UNIQUE (source_file, source_row)
);
