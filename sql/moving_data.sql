-- 1. Populate the Time Dimension (Manual since it's a snapshot)
INSERT INTO dim_time (date, month, month_name, quarter, year)
	VALUES ('2025-12-01', 12, 'December', 4, 2025);

-- 2. Populate the Location Dimension
INSERT INTO dim_location (address, city, state, postal_code, country, region)
	SELECT DISTINCT address, city, state, postal_code, country_region, region 
	FROM staging_imap;

-- 3. Populate the Factory Dimension
INSERT INTO dim_factory (factory_name, factory_type, supplier_group, brand, event)
	SELECT DISTINCT factory_name, factory_type, supplier_group, brand, event 
	FROM staging_imap;

-- 4. Populate the Product Dimension
INSERT INTO dim_product (product_type)
	SELECT DISTINCT product_type FROM staging_imap;

-- 5. Populate the Fact Table (The Join) / This query connects all the IDs together and calculates the worker counts:
INSERT INTO fact_labor_metrics (time_id, location_id, factory_id, product_id, total_workers, line_workers, female_workers_count, migrant_workers_count)
SELECT 
    (SELECT time_id FROM dim_time WHERE month = 12 AND year = 2025),
    l.location_id,
    f.factory_id,
    p.product_id,
    s.total_workers,
    s.line_workers,
    (s.total_workers * s.pct_female / 100)::INT,
    (s.total_workers * s.pct_migrant / 100)::INT
FROM staging_imap s
JOIN dim_location l ON s.address = l.address AND s.city = l.city
JOIN dim_factory f ON s.factory_name = f.factory_name
JOIN dim_product p ON s.product_type = p.product_type;