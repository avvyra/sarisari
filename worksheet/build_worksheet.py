"""Fill the supplied worksheet with screenshots from this machine's actual runs."""
import sys
from datetime import date
from io import BytesIO
from pathlib import Path

from docx import Document
from docx.shared import Inches, RGBColor
from docx.oxml import OxmlElement
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'worksheet' / 'evidence'
TEMPLATE = Path.home() / 'Downloads' / 'Student Lab Worksheet.docx'
PRIVACY_COPY = '--redact-username' in sys.argv
OUTPUT = ROOT / 'worksheet' / ('Masiglat_Alea_ETL-Lab_COPY.docx' if PRIVACY_COPY
                              else 'Masiglat_Alea_ETL-Lab.docx')

doc = Document(TEMPLATE)
doc.tables[0].cell(1, 0).text = 'Alea Masiglat'
doc.tables[0].cell(1, 1).text = '3CSC'
doc.tables[0].cell(1, 2).text = str(date.today())

content = [
    (
        'I ran docker compose version on this PC and got v5.6.0. Then I ran the hello-world container twice. The first run downloaded the image because it was not stored locally, while the second run printed the same greeting without downloading it again. The terminal capture also shows my Windows prompt. This confirmed that the Docker command can reach the daemon, pull an image, create a container, and return its output. I needed that working before starting the PostgreSQL and Airflow containers for the lab.',
        'The first run says the image is missing locally because the Docker image cache is empty. Docker pulls it from Docker Hub and saves it. The second greeting in my screenshot came from the cached image, so there was no second pull.',
    ),
    (
        'I started the services from compose.yaml with a local .env file holding the database and Airflow credentials. The screenshot of docker compose ps shows five running services: the Airflow metadata database, the warehouse database, the scheduler, the triggerer, and the webserver. Both PostgreSQL services report healthy. airflow-init is not listed because it ran once, migrated Airflow metadata, created the admin user, and exited successfully before the remaining Airflow services started. The warehouse schema was initialized from sql/01_schema.sql when its database volume was first created.',
        'Airflow needs a database for schedules, users, and task state. The warehouse stores sales facts and dimensions. Using separate PostgreSQL containers keeps a warehouse reset or a heavy sales query from affecting the Airflow scheduler. Both healthy database statuses are visible in the screenshot.',
    ),
    (
        'I triggered sarisari_sales_etl twice with the starter landing file in place. In the Graph screenshot, I selected the second run and saw extract, transform, load, and quality_check all marked successful. The chain is defined in dags/sarisari_sales_etl.py by check_task().set_upstream(load_task(transform_task(extract_task()))). Passing the previous task output into the next task creates the first three dependencies, and set_upstream connects load to the quality check. This ordering matters because the quality task should inspect the warehouse only after the staged records have been loaded.',
        'The extract task returns a raw-file directory, and transform returns a staged Parquet path. Only those short paths pass through Airflow XCom. Sending a whole DataFrame would store bulky data in Airflow metadata; the shared data-lake volume is the proper place for the actual records.',
    ),
    (
        'The first transform log shows 16 input rows, 13 kept, and three rejected. In data/landing/sales_2026-10.csv, S014 has an empty unit_price, a second S005 repeats an existing sale_id, and S015 has a negative quantity. The log names the source file and line number for each rejection, followed by the summary count. dags/pipeline.py validates required fields, positive numeric values, dates, and duplicate identifiers before writing accepted lines to Parquet. I captured this first-run log before copying the new store batch into the landing folder, so its counts are not mixed with the exercise batch.',
        'I would quarantine rejected lines when they might represent real sales that staff can repair, or when an audit requires the original bad input and its rejection reason. Dropping a bad line prevents it from entering the fact table, but a quarantine record would make later correction easier.',
    ),
    (
        'I ran find datalake -type f after the two starter-batch DAG runs. My screenshot shows a raw CSV and a staged Parquet file under separate run folders for each run. The raw copy keeps the landing bytes intact, including the three dirty lines. The staged file contains only the 13 accepted receipt lines in a columnar format used by the load task. Keeping the run ID in each path prevents one run from overwriting another run’s evidence. This is why the screenshot has four files before the extra store batch is added.',
        'The clean warehouse cannot replace the raw zone. If a validation rule is wrong or the business changes its definition of a sale, I can inspect the original source and rerun the transform. The raw paths in the screenshot are a record of what each run actually received.',
    ),
    (
        'I queried dw.fact_sales and grouped line_total by store using the store surrogate key. The terminal result shows 13 fact rows. Aling Nena Store sold PHP 456, Kuya Jo Mini Mart sold PHP 328, and Mang Ben Sari-Sari sold PHP 253. Adding those three grouped totals gives PHP 1,037, which matches the separate grand-total query in the same screenshot. The SQL schema sets the fact grain at one unique source sale_id and links each sale to store_key and product_key. The rejected CSV lines do not contribute to these figures.',
        'A receipt or basket identifier linked to an anonymized customer dimension would help answer the Sunday rice question. Several separate buyers would suggest a weekend pattern, while one unusually large basket would suggest an isolated purchase. The screenshot shows totals by store, but those totals alone cannot identify buyers.',
    ),
    (
        'The Grid capture shows two green starter-batch runs. I queried the warehouse again after the second run, and the accompanying terminal capture still reports 13 fact rows. The load is safe to repeat because dimension inserts use unique natural keys, while fact_sales has a UNIQUE sale_id and an ON CONFLICT upsert that changes a fact only when its values differ. The second run therefore reused the same store and product rows instead of adding duplicate receipts. I kept the new store CSV outside data/landing until after this check so the 13-row result is meaningful.',
        'With the current ON CONFLICT (sale_id) SQL, removing the fact UNIQUE constraint would make PostgreSQL reject the load because there would be no matching conflict target. If both the constraint and upsert were removed, three full loads of the 13 starter lines would produce 39 fact rows and triple the sales totals.',
    ),
    (
        'I copied sales_2026-10b.csv into data/landing and triggered a new run. It adds Tindahan ni Alea in Barangay San Isidro with six valid sales and two deliberately dirty lines. S022 has a blank price, and the second S018 duplicates a sale_id. The transform log names both new rejections and reports 24 input rows, 19 kept, and five rejected across the two landing files. The dimension query shows four stores. A second query shows that the original 13 facts still total PHP 1,037, while the full fact table has 19 rows.',
        'I think a missing price is more likely than an exact duplicate row in a small store. A cashier may skip a field or export an incomplete price list. I would require a positive price in the sales entry screen before allowing a receipt to be saved, while keeping the pipeline check as a backup.',
    ),
    (
        'I added a fifth rule to quality_check requiring every unit_price to be no more than PHP 10,000. I appended S023 with a price of PHP 12,000 to the new store CSV and triggered a run. The failed screenshot shows the red quality_check task and the exact error for that limit. I changed only S023’s price to PHP 120 and triggered a fresh run; the second screenshot shows the green task and all five checks passing. The corrected upsert left 20 fact rows and kept the original 13 unchanged. Because quality_check follows load, the bad row was briefly committed until the correcting run.',
        'I would stop publication for a price this far outside the store’s normal range and notify the person responsible for the source file. A minor optional-field problem could alert someone without blocking a report. The store owner and data owner should agree on which rules block delivery. The red run in my screenshot signals a problem, but it does not roll back the earlier load.',
    ),
]

images = {
    1: [('cp01_docker.png', (0, 0, 1050, 395), 6.0)],
    2: [('cp02_stack.png', (0, 0, 1000, 290), 6.0)],
    3: [('cp03_graph.png', (0, 0, 1915, 910), 6.15)],
    4: [('cp04_transform.png', (0, 0, 1690, 795), 6.15)],
    5: [('cp05_lake.png', (0, 0, 950, 275), 6.0)],
    6: [('cp06_warehouse.png', (0, 0, 850, 545), 5.7)],
    7: [('cp07_grid.png', (0, 0, 1400, 785), 6.0),
        ('cp07_count.png', (0, 0, 690, 265), 5.4)],
    8: [('cp08_transform.png', (0, 0, 1690, 815), 6.1),
        ('cp08_stores.png', (0, 0, 830, 560), 5.5)],
    9: [('cp09_failed.png', (0, 0, 1690, 1020), 6.1),
        ('cp09_fixed.png', (0, 0, 1690, 795), 6.1)],
}


# Exact locations of the account name in the terminal captures, not the output values.
ACCOUNT_MASKS = {
    'cp01_docker.png': [(7, 48, 78, 75), (150, 346, 218, 373)],
    'cp02_stack.png': [(150, 254, 218, 281)],
    'cp05_lake.png': [(150, 208, 218, 235)],
    'cp06_warehouse.png': [(150, 506, 218, 533)],
    'cp07_count.png': [(150, 231, 218, 258)],
    'cp08_stores.png': [(150, 529, 218, 556)],
}


def add_image(cell, name, crop, width):
    with Image.open(EVIDENCE / name) as original:
        image = original.crop(crop)
        if PRIVACY_COPY:
            for x0, y0, x1, y1 in ACCOUNT_MASKS.get(name, []):
                image.paste((75, 75, 75), (x0 - crop[0], y0 - crop[1], x1 - crop[0], y1 - crop[1]))
        stream = BytesIO()
        image.save(stream, format='PNG')
        stream.seek(0)
    cell.paragraphs[-1].add_run().add_picture(stream, width=Inches(width))
    if PRIVACY_COPY and name in ACCOUNT_MASKS:
        cell.add_paragraph('Local account name masked for privacy; the terminal prompt is still visible.')


for number, (explanation, discussion) in enumerate(content, start=1):
    words = len(explanation.split())
    assert 50 <= words <= 150, (number, words)
    if number < 9:
        screenshot_table = 1 + (number - 1) * 3
        explanation_table = screenshot_table + 1
    else:
        screenshot_table = 25
        explanation_table = 27
    for index, (name, crop, width) in enumerate(images[number]):
        cell = doc.tables[screenshot_table + (index if number == 9 else 0)].cell(1, 0)
        if index == 0 or number == 9:
            cell.text = ''
        else:
            cell.add_paragraph()
        add_image(cell, name, crop, width)
    doc.tables[explanation_table].cell(1, 0).text = explanation
    doc.tables[explanation_table + 1].cell(1, 0).text = discussion
    for table_index in (explanation_table, explanation_table + 1):
        for paragraph in doc.tables[table_index].cell(1, 0).paragraphs:
            paragraph.paragraph_format.keep_together = True

for paragraph in doc.paragraphs:
    if paragraph.text.startswith('Checkpoint '):
        paragraph.paragraph_format.page_break_before = True

# The module's worksheet is kept intact, but all editable text stays plain black.
for table in doc.tables:
    for row in table.rows:
        row._tr.get_or_add_trPr().append(OxmlElement('w:cantSplit'))
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    if not run._r.xpath('.//w:drawing'):
                        run.font.color.rgb = RGBColor(0, 0, 0)

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
doc.save(OUTPUT)
print(OUTPUT)
print('explanation word counts:', [len(text.split()) for text, _ in content])
