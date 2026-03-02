-- CreateTable
CREATE TABLE "dim_factory" (
    "factory_id" SERIAL NOT NULL,
    "factory_name" TEXT NOT NULL,
    "factory_type" TEXT,
    "supplier_group" TEXT,
    "brand" TEXT,
    "event" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "dim_factory_pkey" PRIMARY KEY ("factory_id")
);

-- CreateTable
CREATE TABLE "dim_location" (
    "location_id" SERIAL NOT NULL,
    "address" VARCHAR(255) NOT NULL,
    "city" VARCHAR(100) NOT NULL,
    "state" VARCHAR(100) NOT NULL,
    "postal_code" VARCHAR(50) NOT NULL,
    "country" VARCHAR(100) NOT NULL,
    "region" VARCHAR(50) NOT NULL,

    CONSTRAINT "dim_location_pkey" PRIMARY KEY ("location_id")
);

-- CreateTable
CREATE TABLE "dim_product" (
    "product_id" SERIAL NOT NULL,
    "product_type" VARCHAR(100),

    CONSTRAINT "dim_product_pkey" PRIMARY KEY ("product_id")
);

-- CreateTable
CREATE TABLE "dim_time" (
    "time_id" SERIAL NOT NULL,
    "date" DATE NOT NULL,
    "month" INTEGER NOT NULL,
    "month_name" VARCHAR(20) NOT NULL,
    "quarter" INTEGER NOT NULL,
    "year" INTEGER NOT NULL,

    CONSTRAINT "dim_time_pkey" PRIMARY KEY ("time_id")
);

-- CreateTable
CREATE TABLE "fact_labor_metrics" (
    "fact_id" SERIAL NOT NULL,
    "time_id" INTEGER,
    "location_id" INTEGER,
    "factory_id" INTEGER,
    "product_id" INTEGER,
    "total_workers" INTEGER,
    "line_workers" INTEGER,
    "female_workers_count" INTEGER,
    "migrant_workers_count" INTEGER,

    CONSTRAINT "fact_labor_metrics_pkey" PRIMARY KEY ("fact_id")
);

-- CreateTable
CREATE TABLE "staging_imap" (
    "staging_id" SERIAL NOT NULL,
    "factory_name" TEXT,
    "factory_type" TEXT,
    "product_type" TEXT,
    "brand" TEXT,
    "event" TEXT,
    "supplier_group" TEXT,
    "address" TEXT,
    "city" TEXT,
    "state" TEXT,
    "postal_code" TEXT,
    "country_region" TEXT,
    "region" TEXT,
    "total_workers" INTEGER,
    "line_workers" INTEGER,
    "pct_female" DECIMAL(5,2),
    "pct_migrant" DECIMAL(5,2),

    CONSTRAINT "staging_imap_pkey" PRIMARY KEY ("staging_id")
);

-- CreateIndex
CREATE UNIQUE INDEX "dim_factory_factory_name_key" ON "dim_factory"("factory_name");

-- CreateIndex
CREATE UNIQUE INDEX "dim_location_address_city_state_postal_code_country_region_key" ON "dim_location"("address", "city", "state", "postal_code", "country", "region");

-- CreateIndex
CREATE UNIQUE INDEX "dim_product_product_type_key" ON "dim_product"("product_type");

-- CreateIndex
CREATE UNIQUE INDEX "dim_time_date_key" ON "dim_time"("date");

-- CreateIndex
CREATE UNIQUE INDEX "fact_labor_metrics_time_id_location_id_factory_id_product_i_key" ON "fact_labor_metrics"("time_id", "location_id", "factory_id", "product_id");

-- AddForeignKey
ALTER TABLE "fact_labor_metrics" ADD CONSTRAINT "fact_labor_metrics_factory_id_fkey" FOREIGN KEY ("factory_id") REFERENCES "dim_factory"("factory_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "fact_labor_metrics" ADD CONSTRAINT "fact_labor_metrics_location_id_fkey" FOREIGN KEY ("location_id") REFERENCES "dim_location"("location_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "fact_labor_metrics" ADD CONSTRAINT "fact_labor_metrics_product_id_fkey" FOREIGN KEY ("product_id") REFERENCES "dim_product"("product_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "fact_labor_metrics" ADD CONSTRAINT "fact_labor_metrics_time_id_fkey" FOREIGN KEY ("time_id") REFERENCES "dim_time"("time_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
