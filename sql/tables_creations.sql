-- 1. CLEANUP (Optional: Only if you want to restart)
DROP TABLE IF EXISTS fact_labor_metrics;
DROP TABLE IF EXISTS dim_factory;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS staging_imap;

-- create_tables.sql
-- Creates tables to match the provided prisma schema

-- dim_factory
CREATE TABLE IF NOT EXISTS dim_factory (
  factory_id   SERIAL PRIMARY KEY,
  factory_name VARCHAR(255) NOT NULL UNIQUE,
  factory_type TEXT,
  supplier_group TEXT,
  brand TEXT,
  event TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- dim_location
CREATE TABLE IF NOT EXISTS dim_location (
  location_id   SERIAL PRIMARY KEY,
  address       VARCHAR(255) NOT NULL,
  city          VARCHAR(100),
  state         VARCHAR(100),
  postal_code   VARCHAR(50),
  country       VARCHAR(100),
  region        VARCHAR(50),
  CONSTRAINT u_dim_location_unique_address_city_state_postal_country_region
    UNIQUE(address, city, state, postal_code, country, region)
);

-- dim_product
CREATE TABLE IF NOT EXISTS dim_product (
  product_id   SERIAL PRIMARY KEY,
  product_type VARCHAR(100) UNIQUE
);

-- dim_time
CREATE TABLE IF NOT EXISTS dim_time (
  time_id   SERIAL PRIMARY KEY,
  date      DATE NOT NULL UNIQUE,
  month     INT NOT NULL,
  month_name VARCHAR(20),
  quarter   INT,
  year      INT NOT NULL
);

-- staging_imap
CREATE TABLE IF NOT EXISTS staging_imap (
  staging_id     SERIAL PRIMARY KEY,
  factory_name   TEXT,
  factory_type   TEXT,
  product_type   TEXT,
  brand          TEXT,
  event          TEXT,
  supplier_group TEXT,
  address        TEXT,
  city           TEXT,
  state          TEXT,
  postal_code    TEXT,
  country_region TEXT,
  region         TEXT,
  total_workers  INT,
  line_workers   INT,
  pct_female     NUMERIC(5,2),
  pct_migrant    NUMERIC(5,2)
);

-- fact_labor_metrics
CREATE TABLE IF NOT EXISTS fact_labor_metrics (
  fact_id               SERIAL PRIMARY KEY,
  time_id               INT,
  location_id           INT,
  factory_id            INT,
  product_id            INT,
  total_workers         INT,
  line_workers          INT,
  female_workers_count  INT,
  migrant_workers_count INT,
  CONSTRAINT u_fact_unique_time_loc_fact_prod UNIQUE (time_id, location_id, factory_id, product_id),
  CONSTRAINT fk_fact_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id) ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT fk_fact_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id) ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT fk_fact_factory FOREIGN KEY (factory_id) REFERENCES dim_factory(factory_id) ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT fk_fact_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id) ON UPDATE NO ACTION ON DELETE NO ACTION
);