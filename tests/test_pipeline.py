import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
import pipeline

ROOT = Path(__file__).resolve().parents[1]


class PipelineChecks(unittest.TestCase):
    def test_batches_and_immutable_raw(self):
        with tempfile.TemporaryDirectory() as tmp:
            landing = Path(tmp) / 'landing'
            landing.mkdir()
            lake = Path(tmp) / 'datalake'
            (landing / 'sales_2026-10.csv').write_bytes(
                (ROOT / 'data/landing/sales_2026-10.csv').read_bytes())
            with patch.object(pipeline, 'LANDING', landing), patch.object(pipeline, 'LAKE', lake):
                raw = pipeline.extract('first')
                rows, rejected = pipeline.clean_rows(raw)
                self.assertEqual((len(rows), len(rejected)), (13, 3))
                original = {r['sale_id']: r.copy() for r in rows}
                self.assertEqual({r[2] for r in rejected},
                                 {'missing/extra field', 'duplicate sale_id',
                                  'quantity and price must be positive'})
                import pandas as pd
                staged = pipeline.transform(raw, 'first')
                self.assertEqual(len(pd.read_parquet(staged)), 13)
                self.assertEqual(Path(staged).read_bytes()[:4], b'PAR1')
                pipeline.extract('first')  # same source, same bytes: safe retry
                (landing / 'sales_2026-10b.csv').write_bytes(
                    (ROOT / 'data/exercise1/sales_2026-10b.csv').read_bytes())
                rows, rejected = pipeline.clean_rows(pipeline.extract('second'))
                self.assertEqual((len(rows), len(rejected)), (19, 5))
                self.assertEqual({r['sale_id']: r for r in rows if r['sale_id'] in original}, original)
                self.assertEqual(len({(r['store_name'], r['barangay']) for r in rows}), 4)
                self.assertEqual(len(pd.read_parquet(pipeline.transform(lake / 'raw/second', 'second'))), 19)
                with (landing / 'sales_2026-10b.csv').open('a') as file:
                    file.write('S023,2026-10-17,Tindahan ni Alea,Barangay San Isidro,'
                               'Premium Rice,Groceries,1,12000.00\n')
                with self.assertRaisesRegex(ValueError, 'Raw batch changed'):
                    pipeline.extract('second')
                rows, rejected = pipeline.clean_rows(pipeline.extract('third'))
                self.assertEqual((len(rows), len(rejected)), (20, 5))
                self.assertEqual(rows[-1]['unit_price'], '12000.00')

    def test_fifth_check_fails_then_passes(self):
        class FakeCursor:
            def __init__(self, price):
                self.price = price
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def execute(self, query):
                self.passed = self.price <= 10000 if 'unit_price > 10000' in query else True
            def fetchone(self): return (self.passed,)

        class FakeConnection:
            def __init__(self, price): self.price = price
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def cursor(self): return FakeCursor(self.price)

        with patch.object(pipeline, '_connect', return_value=FakeConnection(12000)):
            with self.assertRaisesRegex(ValueError, 'unit_price under or equal'):
                pipeline.quality_check()
        with patch.object(pipeline, '_connect', return_value=FakeConnection(120)):
            pipeline.quality_check()


if __name__ == '__main__':
    unittest.main()
