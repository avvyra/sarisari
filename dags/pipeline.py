"""Sari-sari ETL operations; the Airflow DAG only wires these four functions together."""

import csv
import logging
import os
import re
import shutil
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

LANDING = Path('/opt/airflow/data/landing')
LAKE = Path('/opt/airflow/datalake')
COLUMNS = ('sale_id', 'sale_date', 'store_name', 'barangay', 'product_name',
           'category', 'quantity', 'unit_price')


def _run_dir(run_id):
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', run_id)


def extract(run_id):
    """Copy every available batch byte-for-byte into an immutable raw run partition."""
    batches = sorted(LANDING.glob('sales_*.csv'))
    if not batches:
        raise ValueError('No sales_*.csv files in data/landing')
    target = LAKE / 'raw' / _run_dir(run_id)
    target.mkdir(parents=True, exist_ok=True)
    for source in batches:
        dest = target / source.name
        if dest.exists():
            if dest.read_bytes() != source.read_bytes():
                raise ValueError(f'Raw batch changed during the same run: {dest}')
        else:
            shutil.copyfile(source, dest)
        logging.info('Raw batch: %s', dest)
    return str(target)


def clean_rows(raw_dir):
    """Return clean receipt lines and rejection messages (file, line, reason)."""
    rows, rejected, seen = [], [], set()
    for path in sorted(Path(raw_dir).glob('sales_*.csv')):
        with path.open(newline='', encoding='utf-8-sig') as source:
            reader = csv.DictReader(source)
            if tuple(reader.fieldnames or ()) != COLUMNS:
                raise ValueError(f'Unexpected columns in {path.name}: {reader.fieldnames}')
            for line, raw in enumerate(reader, start=2):
                row = {key: (raw[key] or '').strip() for key in COLUMNS}
                reason = None
                try:
                    if None in raw or any(not row[key] for key in COLUMNS):
                        raise ValueError('missing/extra field')
                    row['sale_date'] = date.fromisoformat(row['sale_date']).isoformat()
                    qty = int(row['quantity'])
                    price = Decimal(row['unit_price'])
                    if qty <= 0 or price <= 0 or not price.is_finite():
                        raise ValueError('quantity and price must be positive')
                    if price.as_tuple().exponent < -2:
                        raise ValueError('unit_price has more than two decimal places')
                    if row['sale_id'] in seen:
                        raise ValueError('duplicate sale_id')
                    row['quantity'] = qty
                    row['unit_price'] = f'{price:.2f}'
                except (ValueError, InvalidOperation, TypeError) as exc:
                    reason = str(exc)
                if reason:
                    rejected.append((path.name, line, reason))
                else:
                    seen.add(row['sale_id'])
                    rows.append(row)
    return rows, rejected


def transform(raw_dir, run_id):
    import pandas as pd

    rows, rejected = clean_rows(raw_dir)
    for filename, line, reason in rejected:
        logging.warning('Rejected %s line %d: %s', filename, line, reason)
    logging.info('Rows in: %d, rows kept: %d, rejected: %d',
                 len(rows) + len(rejected), len(rows), len(rejected))
    if not rows:
        raise ValueError('Every source row was rejected')
    output = LAKE / 'staged' / _run_dir(run_id) / 'sales.parquet'
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=COLUMNS).to_parquet(output, index=False)
    logging.info('Staged Parquet: %s', output)
    return str(output)


def _connect():
    import psycopg2
    return psycopg2.connect(host=os.environ['DW_HOST'], port=os.environ['DW_PORT'],
                            dbname=os.environ['DW_NAME'], user=os.environ['DW_USER'],
                            password=os.environ['DW_PASSWORD'])


def load(staged_path):
    import pandas as pd

    rows = pd.read_parquet(staged_path).to_dict('records')
    with _connect() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute('''INSERT INTO dw.dim_store (store_name, barangay)
                    VALUES (%s, %s) ON CONFLICT (store_name, barangay) DO NOTHING''',
                    (row['store_name'], row['barangay']))
                cur.execute('''INSERT INTO dw.dim_product (product_name, category)
                    VALUES (%s, %s) ON CONFLICT (product_name, category) DO NOTHING''',
                    (row['product_name'], row['category']))
                cur.execute('''INSERT INTO dw.fact_sales
                    (sale_id, sale_date, store_key, product_key, quantity, unit_price, line_total)
                    SELECT %s, %s::date, s.store_key, p.product_key, %s, %s,
                           %s::numeric * %s::numeric
                    FROM dw.dim_store s CROSS JOIN dw.dim_product p
                    WHERE s.store_name = %s AND s.barangay = %s
                      AND p.product_name = %s AND p.category = %s
                    ON CONFLICT (sale_id) DO UPDATE SET
                        sale_date = EXCLUDED.sale_date, store_key = EXCLUDED.store_key,
                        product_key = EXCLUDED.product_key, quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price, line_total = EXCLUDED.line_total
                    WHERE (dw.fact_sales.sale_date, dw.fact_sales.store_key,
                           dw.fact_sales.product_key, dw.fact_sales.quantity,
                           dw.fact_sales.unit_price, dw.fact_sales.line_total)
                       IS DISTINCT FROM
                          (EXCLUDED.sale_date, EXCLUDED.store_key,
                           EXCLUDED.product_key, EXCLUDED.quantity,
                           EXCLUDED.unit_price, EXCLUDED.line_total)''',
                    (row['sale_id'], row['sale_date'], int(row['quantity']),
                     row['unit_price'], int(row['quantity']), row['unit_price'],
                     row['store_name'], row['barangay'], row['product_name'], row['category']))
        # psycopg2 connection context commits the complete load, or rolls it back.
    logging.info('Loaded %d staged receipt lines (upsert by sale_id)', len(rows))


def quality_check():
    checks = (
        ('warehouse contains sales', 'SELECT count(*) > 0 FROM dw.fact_sales'),
        ('store/product keys resolve', '''SELECT count(*) = 0 FROM dw.fact_sales f
            LEFT JOIN dw.dim_store s USING (store_key)
            LEFT JOIN dw.dim_product p USING (product_key)
            WHERE s.store_key IS NULL OR p.product_key IS NULL'''),
        ('quantities and prices positive', '''SELECT count(*) = 0 FROM dw.fact_sales
            WHERE quantity <= 0 OR unit_price <= 0'''),
        ('line totals match', '''SELECT count(*) = 0 FROM dw.fact_sales
            WHERE line_total <> quantity * unit_price'''),
        ('unit_price under or equal to PHP 10000', '''SELECT count(*) = 0 FROM dw.fact_sales
            WHERE unit_price > 10000'''),
    )
    with _connect() as conn:
        with conn.cursor() as cur:
            for name, query in checks:
                cur.execute(query)
                if not cur.fetchone()[0]:
                    logging.error('QUALITY CHECK FAILED: %s', name)
                    raise ValueError(f'QUALITY CHECK FAILED: {name}')
                logging.info('QUALITY CHECK PASSED: %s', name)
