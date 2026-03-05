-- Creates a stored procedure to process all rows in staging_imap for a given month/date
CREATE OR REPLACE FUNCTION process_import(p_time_date DATE)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_time_id INT;
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

  SELECT time_id INTO v_time_id
  FROM dim_time
  WHERE date = p_time_date
  LIMIT 1;

  -- 2) populate dim_location from normalized staging
  INSERT INTO dim_location (address, city, state, postal_code, country, region)
  SELECT DISTINCT
    TRIM(address),
    TRIM(city),
    TRIM(state),
    TRIM(postal_code),
    TRIM(country_region),
    TRIM(region)
  FROM staging_imap
  WHERE COALESCE(TRIM(address), '') <> ''
    AND COALESCE(TRIM(city), '') <> ''
    AND COALESCE(TRIM(state), '') <> ''
    AND COALESCE(TRIM(postal_code), '') <> ''
    AND COALESCE(TRIM(country_region), '') <> ''
    AND COALESCE(TRIM(region), '') <> ''
  ON CONFLICT (address, city, state, postal_code, country, region) DO NOTHING;

  -- 3) populate dim_factory from normalized staging
  INSERT INTO dim_factory (factory_name, factory_type, supplier_group, brand, event)
  SELECT DISTINCT
    TRIM(factory_name),
    NULLIF(TRIM(factory_type), ''),
    NULLIF(TRIM(supplier_group), ''),
    NULLIF(TRIM(brand), ''),
    NULLIF(TRIM(event), '')
  FROM staging_imap
  WHERE COALESCE(TRIM(factory_name), '') <> ''
  ON CONFLICT (factory_name) DO NOTHING;

  -- 4) populate dim_product from normalized staging
  INSERT INTO dim_product (product_type)
  SELECT DISTINCT TRIM(product_type)
  FROM staging_imap
  WHERE COALESCE(TRIM(product_type), '') <> ''
  ON CONFLICT (product_type) DO NOTHING;

  -- 5) populate fact_labor_metrics (aggregate by fact grain)
  WITH s AS (
    SELECT
      TRIM(factory_name) AS factory_name,
      TRIM(product_type) AS product_type,
      TRIM(address) AS address,
      TRIM(city) AS city,
      TRIM(state) AS state,
      TRIM(postal_code) AS postal_code,
      TRIM(country_region) AS country_region,
      TRIM(region) AS region,
      total_workers,
      line_workers,
      pct_female,
      pct_migrant
    FROM staging_imap
    WHERE COALESCE(TRIM(factory_name), '') <> ''
      AND COALESCE(TRIM(product_type), '') <> ''
      AND COALESCE(TRIM(address), '') <> ''
      AND COALESCE(TRIM(city), '') <> ''
      AND COALESCE(TRIM(state), '') <> ''
      AND COALESCE(TRIM(postal_code), '') <> ''
      AND COALESCE(TRIM(country_region), '') <> ''
      AND COALESCE(TRIM(region), '') <> ''
  )
  INSERT INTO fact_labor_metrics (
    time_id, location_id, factory_id, product_id,
    total_workers, line_workers, female_workers_count, migrant_workers_count
  )
  SELECT
    v_time_id AS time_id,
    l.location_id,
    f.factory_id,
    p.product_id,
    SUM(s.total_workers)::INT AS total_workers,
    SUM(s.line_workers)::INT AS line_workers,
    SUM(
      CASE
        WHEN s.total_workers IS NULL OR s.pct_female IS NULL THEN 0
        ELSE (s.total_workers * s.pct_female / 100.0)
      END
    )::INT AS female_workers_count,
    SUM(
      CASE
        WHEN s.total_workers IS NULL OR s.pct_migrant IS NULL THEN 0
        ELSE (s.total_workers * s.pct_migrant / 100.0)
      END
    )::INT AS migrant_workers_count
  FROM s
  JOIN dim_location l
    ON s.address = l.address
    AND s.city = l.city
    AND s.state = l.state
    AND s.postal_code = l.postal_code
    AND s.country_region = l.country
    AND s.region = l.region
  JOIN dim_factory f ON s.factory_name = f.factory_name
  JOIN dim_product p ON s.product_type = p.product_type
  GROUP BY l.location_id, f.factory_id, p.product_id
  ON CONFLICT (time_id, location_id, factory_id, product_id) DO UPDATE
    SET total_workers = EXCLUDED.total_workers,
        line_workers = EXCLUDED.line_workers,
        female_workers_count = EXCLUDED.female_workers_count,
        migrant_workers_count = EXCLUDED.migrant_workers_count;

  -- 6) clean staging (fast and simple)
  TRUNCATE TABLE staging_imap;
END;
$$;