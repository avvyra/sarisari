CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.dim_store (
    store_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name TEXT NOT NULL,
    barangay TEXT NOT NULL,
    UNIQUE (store_name, barangay)
);

CREATE TABLE dw.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    UNIQUE (product_name, category)
);

-- Grain: exactly one source sale_id/receipt line, across all batches.
CREATE TABLE dw.fact_sales (
    sale_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sale_id TEXT NOT NULL UNIQUE,
    sale_date DATE NOT NULL,
    store_key BIGINT NOT NULL REFERENCES dw.dim_store(store_key),
    product_key BIGINT NOT NULL REFERENCES dw.dim_product(product_key),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    line_total NUMERIC(14,2) NOT NULL,
    CHECK (line_total = quantity * unit_price)
);
