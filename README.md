# ETL Cuaca: CSV ke Parquet ke BigQuery

Pipeline ETL sederhana untuk data cuaca harian menggunakan Polars dan Google BigQuery.

## Alur data
CSV (data/raw/cuaca.csv) -> Polars (extract, transform) -> Parquet (data/processed/) -> BigQuery (etl-latihan.cuaca_latihan.data_cuaca)

## Cara menjalankan
python -m venv venv
source venv/bin/activate
pip install polars pyarrow google-cloud-bigquery
gcloud auth application-default login
python src/pipeline.py

## Sumber data
Dataset cuaca harian dari Kaggle. Letakkan file CSV di data/raw/cuaca.csv.
