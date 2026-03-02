-- Creates a stored procedure to process all rows in staging_imap for a given month/date
CREATE OR REPLACE FUNCTION process_import(p_time_date DATE)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- 1) ensure time dimension row exists (idempotent)
  INSERT INTO dim_time (date, month, month_name, quarter, year)
  VALUES (
    p_time_date,
    EXTRACT(MONTH FROM p_time_date)::INT,
    to_char(p_time_date, 'TMMonth'),
    CEIL(EXTRACT(MONTH FROM p_time_date) / 3.0)::INT,
    EXTRACT(YEAR FROM p_time_date)::INT
  )
  ON CONFLICT (date) DO NOTHING;

  -- 2) populate dim_location from staging (only non-null address rows)
  INSERT INTO dim_location (address, city, state, postal_code, country, region)
  SELECT DISTINCT address, city, state, postal_code, country_region, region
  FROM staging_imap
  WHERE address IS NOT NULL
  ON CONFLICT (address, city, state, postal_code, country, region) DO NOTHING;

  -- 3) populate dim_factory from staging (only non-null factory_name)
  INSERT INTO dim_factory (factory_name, factory_type, supplier_group, brand, event)
  SELECT DISTINCT factory_name, factory_type, supplier_group, brand, event
  FROM staging_imap
  WHERE factory_name IS NOT NULL
  ON CONFLICT (factory_name) DO NOTHING;

  -- 4) populate dim_product from staging (only non-null product_type)
  INSERT INTO dim_product (product_type)
  SELECT DISTINCT product_type
  FROM staging_imap
  WHERE product_type IS NOT NULL
  ON CONFLICT (product_type) DO NOTHING;

  -- 5) populate fact_labor_metrics (join staging -> dims)
  INSERT INTO fact_labor_metrics (
    time_id, location_id, factory_id, product_id,
    total_workers, line_workers, female_workers_count, migrant_workers_count
  )
  SELECT
    (SELECT time_id FROM dim_time WHERE date = p_time_date LIMIT 1) AS time_id,
    l.location_id,
    f.factory_id,
    p.product_id,
    s.total_workers,
    s.line_workers,
    -- compute female_workers_count safely (null if missing)
    CASE
      WHEN s.total_workers IS NULL OR s.pct_female IS NULL THEN NULL
      ELSE (s.total_workers * s.pct_female / 100.0)::INT
    END AS female_workers_count,
    -- compute migrant_workers_count safely (null if missing)
    CASE
      WHEN s.total_workers IS NULL OR s.pct_migrant IS NULL THEN NULL
      ELSE (s.total_workers * s.pct_migrant / 100.0)::INT
    END AS migrant_workers_count
  FROM staging_imap s
  JOIN dim_location l
    ON s.address = l.address
    AND (s.city IS NOT DISTINCT FROM l.city)
    AND (s.state IS NOT DISTINCT FROM l.state)
    AND (s.postal_code IS NOT DISTINCT FROM l.postal_code)
    AND (s.country_region IS NOT DISTINCT FROM l.country)
    AND (s.region IS NOT DISTINCT FROM l.region)
  JOIN dim_factory f ON s.factory_name = f.factory_name
  JOIN dim_product p ON s.product_type = p.product_type
  ON CONFLICT (time_id, location_id, factory_id, product_id) DO NOTHING;

  -- 6) clean staging (fast and simple)
  TRUNCATE TABLE staging_imap;
END;
$$;