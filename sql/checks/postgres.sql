SELECT count(*) AS sales, sum(sale_quantity) AS units,
       sum(sale_total_price) AS revenue
FROM fact_sales;

SELECT source_file, count(*) AS sales
FROM fact_sales
GROUP BY source_file
ORDER BY source_file;

SELECT 'customers' AS entity, count(*) AS rows FROM dim_customer
UNION ALL SELECT 'sellers', count(*) FROM dim_seller
UNION ALL SELECT 'products', count(*) FROM dim_product
UNION ALL SELECT 'stores', count(*) FROM dim_store
UNION ALL SELECT 'suppliers', count(*) FROM dim_supplier
UNION ALL SELECT 'dates', count(*) FROM dim_date
UNION ALL SELECT 'sales', count(*) FROM fact_sales;

SELECT p.product_name, sum(f.sale_quantity) AS units,
       sum(f.sale_total_price) AS revenue
FROM fact_sales f
JOIN dim_product p USING (product_key)
GROUP BY p.product_name
ORDER BY revenue DESC;

DO $$
DECLARE
    sales BIGINT;
    units BIGINT;
    revenue NUMERIC;
BEGIN
    SELECT count(*), sum(sale_quantity), sum(sale_total_price)
    INTO sales, units, revenue FROM fact_sales;
    IF sales <> 10000 OR units IS DISTINCT FROM 54623::BIGINT
       OR revenue IS DISTINCT FROM 2529852.12::NUMERIC THEN
        RAISE EXCEPTION 'Validation failed: sales=%, units=%, revenue=%', sales, units, revenue;
    END IF;
    IF (SELECT count(*) FROM (
        SELECT source_file FROM fact_sales GROUP BY source_file HAVING count(*) = 1000
    ) files) <> 10 THEN
        RAISE EXCEPTION 'Expected 10 files with 1000 sales each';
    END IF;
    IF EXISTS (
        SELECT 1 FROM fact_sales f
        JOIN dim_customer c USING (customer_key)
        JOIN dim_seller s USING (seller_key)
        JOIN dim_product p USING (product_key)
        JOIN dim_date d USING (date_key)
        WHERE f.source_row_id <> c.source_row_id OR f.source_row_id <> s.source_row_id
           OR f.source_row_id <> p.source_row_id
           OR d.date_key <> to_char(d.full_date, 'YYYYMMDD')::INTEGER
    ) THEN
        RAISE EXCEPTION 'Invalid dimension mapping';
    END IF;
    RAISE NOTICE 'OK: % sales, % units, revenue %', sales, units, revenue;
END $$;
