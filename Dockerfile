FROM apache/airflow:2.10.5-python3.11
RUN pip install --no-cache-dir 'pandas>=2.2,<3' 'pyarrow>=18,<22' 'psycopg2-binary>=2.9,<3'
