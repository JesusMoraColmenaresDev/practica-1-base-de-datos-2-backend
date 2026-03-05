-- Apply additional canonicalization rules:
-- - dim_factory.factory_type: upper
-- - dim_factory.brand: upper
-- - dim_factory.supplier_group: upper
-- - dim_factory.event: lower
-- - dim_product.product_type: lower
-- - dim_location.region: upper
-- and ensure process_import enforces the same rules for future loads.

-- 1) Merge product variants that collide after lower-casing product_type.
WITH product_canon AS (
  SELECT
    product_id,
    LOWER(TRIM(product_type)) AS canonical_type,
    MIN(product_id) OVER (PARTITION BY LOWER(TRIM(product_type))) AS canonical_id
  FROM dim_product
  WHERE product_type IS NOT NULL
    AND TRIM(product_type) <> ''
),
conflicts AS (
  SELECT
    src.fact_id AS source_fact_id,
    tgt.fact_id AS target_fact_id
  FROM fact_labor_metrics src
  JOIN product_canon pc
    ON src.product_id = pc.product_id
   AND src.product_id <> pc.canonical_id
  JOIN fact_labor_metrics tgt
    ON tgt.time_id IS NOT DISTINCT FROM src.time_id
   AND tgt.location_id IS NOT DISTINCT FROM src.location_id
   AND tgt.factory_id IS NOT DISTINCT FROM src.factory_id
   AND tgt.product_id = pc.canonical_id
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

WITH product_canon AS (
  SELECT
    product_id,
    MIN(product_id) OVER (PARTITION BY LOWER(TRIM(product_type))) AS canonical_id
  FROM dim_product
  WHERE product_type IS NOT NULL
    AND TRIM(product_type) <> ''
)
UPDATE fact_labor_metrics m
SET product_id = pc.canonical_id
FROM product_canon pc
WHERE m.product_id = pc.product_id
  AND pc.product_id <> pc.canonical_id
  AND NOT EXISTS (
    SELECT 1
    FROM fact_labor_metrics e
    WHERE e.fact_id <> m.fact_id
      AND e.time_id IS NOT DISTINCT FROM m.time_id
      AND e.location_id IS NOT DISTINCT FROM m.location_id
      AND e.factory_id IS NOT DISTINCT FROM m.factory_id
      AND e.product_id = pc.canonical_id
  );

WITH product_canon AS (
  SELECT
    product_id,
    MIN(product_id) OVER (PARTITION BY LOWER(TRIM(product_type))) AS canonical_id
  FROM dim_product
  WHERE product_type IS NOT NULL
    AND TRIM(product_type) <> ''
)
DELETE FROM dim_product d
USING product_canon pc
WHERE d.product_id = pc.product_id
  AND pc.product_id <> pc.canonical_id;

UPDATE dim_product
SET product_type = LOWER(TRIM(product_type))
WHERE product_type IS NOT NULL;

-- 2) Merge location variants that collide after upper-casing region.
WITH location_canon AS (
  SELECT
    location_id,
    MIN(location_id) OVER (
      PARTITION BY
        LOWER(TRIM(address)),
        UPPER(TRIM(SPLIT_PART(city, '/', 1))),
        LOWER(TRIM(state)),
        TRIM(postal_code),
        UPPER(TRIM(country)),
        UPPER(TRIM(region))
    ) AS canonical_id
  FROM dim_location
),
conflicts AS (
  SELECT
    src.fact_id AS source_fact_id,
    tgt.fact_id AS target_fact_id
  FROM fact_labor_metrics src
  JOIN location_canon lc
    ON src.location_id = lc.location_id
   AND src.location_id <> lc.canonical_id
  JOIN fact_labor_metrics tgt
    ON tgt.time_id IS NOT DISTINCT FROM src.time_id
   AND tgt.factory_id IS NOT DISTINCT FROM src.factory_id
   AND tgt.product_id IS NOT DISTINCT FROM src.product_id
   AND tgt.location_id = lc.canonical_id
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

WITH location_canon AS (
  SELECT
    location_id,
    MIN(location_id) OVER (
      PARTITION BY
        LOWER(TRIM(address)),
        UPPER(TRIM(SPLIT_PART(city, '/', 1))),
        LOWER(TRIM(state)),
        TRIM(postal_code),
        UPPER(TRIM(country)),
        UPPER(TRIM(region))
    ) AS canonical_id
  FROM dim_location
)
UPDATE fact_labor_metrics m
SET location_id = lc.canonical_id
FROM location_canon lc
WHERE m.location_id = lc.location_id
  AND lc.location_id <> lc.canonical_id
  AND NOT EXISTS (
    SELECT 1
    FROM fact_labor_metrics e
    WHERE e.fact_id <> m.fact_id
      AND e.time_id IS NOT DISTINCT FROM m.time_id
      AND e.factory_id IS NOT DISTINCT FROM m.factory_id
      AND e.product_id IS NOT DISTINCT FROM m.product_id
      AND e.location_id = lc.canonical_id
  );

WITH location_canon AS (
  SELECT
    location_id,
    MIN(location_id) OVER (
      PARTITION BY
        LOWER(TRIM(address)),
        UPPER(TRIM(SPLIT_PART(city, '/', 1))),
        LOWER(TRIM(state)),
        TRIM(postal_code),
        UPPER(TRIM(country)),
        UPPER(TRIM(region))
    ) AS canonical_id
  FROM dim_location
)
DELETE FROM dim_location d
USING location_canon lc
WHERE d.location_id = lc.location_id
  AND lc.location_id <> lc.canonical_id;

UPDATE dim_location
SET address = LOWER(TRIM(address)),
    city = UPPER(TRIM(SPLIT_PART(city, '/', 1))),
    state = LOWER(TRIM(state)),
    country = UPPER(TRIM(country)),
    region = UPPER(TRIM(region));

-- 3) Canonicalize non-key descriptive factory columns.
UPDATE dim_factory
SET factory_name = UPPER(TRIM(factory_name)),
    factory_type = NULLIF(UPPER(TRIM(factory_type)), ''),
    supplier_group = NULLIF(UPPER(TRIM(supplier_group)), ''),
    brand = NULLIF(UPPER(TRIM(brand)), ''),
    event = NULLIF(LOWER(TRIM(event)), '');

-- 4) Update import function to enforce all canonicalization rules.
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
    LOWER(TRIM(address)),
    UPPER(TRIM(SPLIT_PART(city, '/', 1))),
    LOWER(TRIM(state)),
    TRIM(postal_code),
    UPPER(TRIM(country_region)),
    UPPER(TRIM(region))
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
    NULLIF(UPPER(TRIM(factory_type)), ''),
    NULLIF(UPPER(TRIM(supplier_group)), ''),
    NULLIF(UPPER(TRIM(brand)), ''),
    NULLIF(LOWER(TRIM(event)), '')
  FROM staging_imap
  WHERE COALESCE(TRIM(factory_name), '') <> ''
  ON CONFLICT (factory_name) DO NOTHING;

  INSERT INTO dim_product (product_type)
  SELECT DISTINCT LOWER(TRIM(product_type))
  FROM staging_imap
  WHERE COALESCE(TRIM(product_type), '') <> ''
  ON CONFLICT (product_type) DO NOTHING;

  WITH s AS (
    SELECT
      UPPER(TRIM(factory_name)) AS factory_name,
      LOWER(TRIM(product_type)) AS product_type,
      LOWER(TRIM(address)) AS address,
      UPPER(TRIM(SPLIT_PART(city, '/', 1))) AS city,
      LOWER(TRIM(state)) AS state,
      TRIM(postal_code) AS postal_code,
      UPPER(TRIM(country_region)) AS country_region,
      UPPER(TRIM(region)) AS region,
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
