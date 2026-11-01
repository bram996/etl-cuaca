import logging
import os

import polars as pl
from google.cloud import bigquery

CSV_PATH = "data/raw/cuaca.csv"
PARQUET_PATH = "data/processed/data_cuaca.parquet"
DATASET_ID = "cuaca_latihan"
TABLE_NAME = "data_cuaca"
DATASET_LOCATION = "asia-southeast2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("etl_cuaca")


def extract(path: str) -> pl.DataFrame:
    log.info("Extract: membaca %s", path)
    df = pl.read_csv(path, infer_schema_length=10000)
    log.info("Extract selesai: %s baris, %s kolom", *df.shape)
    return df


def transform(df: pl.DataFrame) -> pl.DataFrame:
    log.info("Transform: mulai")

    # Rapikan nama kolom: hapus spasi di awal/akhir, ganti spasi jadi underscore
    df = df.rename({c: c.strip().replace(" ", "_") for c in df.columns})
    # Ganti "/" agar valid untuk BigQuery
    df = df.rename({c: c.replace("/", "_per_") for c in df.columns})

    # Ubah kolom tanggal dari string ke Date
    df = df.with_columns(pl.col("CET").str.to_date("%Y-%m-%d", strict=False))

    # Buang baris tanpa tanggal, lalu hapus duplikat
    df = df.drop_nulls(subset=["CET"]).unique()

    # Tambah kolom turunan
    df = df.with_columns(
        pl.col("CET").dt.year().alias("tahun"),
        pl.col("CET").dt.month().alias("bulan"),
    )

    log.info("Transform selesai: %s baris, %s kolom", *df.shape)
    return df


def save_parquet(df: pl.DataFrame, path: str) -> None:
    log.info("Save: menulis Parquet ke %s", path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.write_parquet(path, compression="zstd")
    csv_mb = os.path.getsize(CSV_PATH) / 1e6
    parquet_mb = os.path.getsize(path) / 1e6
    log.info("Ukuran CSV %.2f MB, Parquet %.2f MB", csv_mb, parquet_mb)


def load_to_bigquery(path: str) -> None:
    log.info("Load: mulai ke BigQuery")
    try:
        client = bigquery.Client()
        project_id = client.project

        dataset = bigquery.Dataset(f"{project_id}.{DATASET_ID}")
        dataset.location = DATASET_LOCATION
        client.create_dataset(dataset, exists_ok=True)

        table_id = f"{project_id}.{DATASET_ID}.{TABLE_NAME}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with open(path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

        num_rows = client.get_table(table_id).num_rows
        log.info("Load selesai: %s baris ke %s", num_rows, table_id)
    except Exception:
        log.exception("Load ke BigQuery gagal")
        raise


def main():
    raw = extract(CSV_PATH)
    clean = transform(raw)
    save_parquet(clean, PARQUET_PATH)
    load_to_bigquery(PARQUET_PATH)


if __name__ == "__main__":
    main()
