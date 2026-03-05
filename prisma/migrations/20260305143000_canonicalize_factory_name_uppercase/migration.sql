-- Canonicalize factories to uppercase and merge case-variant duplicates.
-- This ensures one factory key regardless of text casing.

-- 1) Merge fact rows that would collide after remapping factory_id to canonical uppercase key.
WITH factory_canon AS (
  SELECT
    factory_id,
    UPPER(TRIM(factory_name)) AS canonical_name,
    MIN(factory_id) OVER (PARTITION BY UPPER(TRIM(factory_name))) AS canonical_id
  FROM dim_factory
),
conflicts AS (
  SELECT
    src.fact_id AS source_fact_id,
    tgt.fact_id AS target_fact_id
  FROM fact_labor_metrics src
  JOIN factory_canon fc
    ON src.factory_id = fc.factory_id
   AND src.factory_id <> fc.canonical_id
  JOIN fact_labor_metrics tgt
    ON tgt.time_id IS NOT DISTINCT FROM src.time_id
   AND tgt.location_id IS NOT DISTINCT FROM src.location_id
   AND tgt.product_id IS NOT DISTINCT FROM src.product_id
   AND tgt.factory_id = fc.canonical_id
),
merged_targets AS (
  UPDATE fact_labor_metrics tgt
  SET total_workers = COALESCE(tgt.total_workers, 0) + COALESCE(src.total_workers, 0),
      line_workers = COALESCE(tgt.line_workers, 0) + COALESCE(src.line_workers, 0),
      female_workers_count = COALESCE(tgt.female_workers_count, 0) + COALESCE(src.female_workers_count, 0),
      migrant_workers_count = COALESCE(tgt.migrant_workers_count, 0) + COALESCE(src.migrant_workers_count, 0)
  FROM conflicts c
  JOIN fact_labor_metrics src ON src.fact_id = c.source_fact_id
  WHERE tgt.fact_id = c.target_fact_id
  RETURNING c.source_fact_id
)
DELETE FROM fact_labor_metrics src
USING merged_targets mt
WHERE src.fact_id = mt.source_fact_id;

-- 2) Remap remaining fact rows to canonical factory_id.
WITH factory_canon AS (
  SELECT
    factory_id,
    MIN(factory_id) OVER (PARTITION BY UPPER(TRIM(factory_name))) AS canonical_id
  FROM dim_factory
)
UPDATE fact_labor_metrics m
SET factory_id = fc.canonical_id
FROM factory_canon fc
WHERE m.factory_id = fc.factory_id
  AND fc.factory_id <> fc.canonical_id
  AND NOT EXISTS (
    SELECT 1
    FROM fact_labor_metrics e
    WHERE e.fact_id <> m.fact_id
      AND e.time_id IS NOT DISTINCT FROM m.time_id
      AND e.location_id IS NOT DISTINCT FROM m.location_id
      AND e.product_id IS NOT DISTINCT FROM m.product_id
      AND e.factory_id = fc.canonical_id
  );

-- 3) Remove duplicate dim_factory rows after remap.
WITH factory_canon AS (
  SELECT
    factory_id,
    MIN(factory_id) OVER (PARTITION BY UPPER(TRIM(factory_name))) AS canonical_id
  FROM dim_factory
)
DELETE FROM dim_factory d
USING factory_canon fc
WHERE d.factory_id = fc.factory_id
  AND fc.factory_id <> fc.canonical_id;

-- 4) Canonicalize existing names.
UPDATE dim_factory
SET factory_name = UPPER(TRIM(factory_name));

-- 5) Ensure import function always writes/joins uppercase canonical factory names.
CREATE OR REPLACE FUNCTION public.process_import(p_time_date DATE)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_time_id INT;
BEGIN
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

  INSERT INTO dim_factory (factory_name, factory_type, supplier_group, brand, event)
  SELECT DISTINCT
    UPPER(TRIM(factory_name)),
    NULLIF(TRIM(factory_type), ''),
    NULLIF(TRIM(supplier_group), ''),
    NULLIF(TRIM(brand), ''),
    NULLIF(TRIM(event), '')
  FROM staging_imap
  WHERE COALESCE(TRIM(factory_name), '') <> ''
  ON CONFLICT (factory_name) DO NOTHING;

  INSERT INTO dim_product (product_type)
  SELECT DISTINCT TRIM(product_type)
  FROM staging_imap
  WHERE COALESCE(TRIM(product_type), '') <> ''
  ON CONFLICT (product_type) DO NOTHING;

  WITH s AS (
    SELECT
      UPPER(TRIM(factory_name)) AS factory_name,
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

  TRUNCATE TABLE staging_imap;
END;
$$;
