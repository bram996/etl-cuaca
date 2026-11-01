from google.cloud import bigquery

client = bigquery.Client()
query = """
SELECT
  COUNT(*) AS jumlah_baris,
  MIN(CET) AS tanggal_awal,
  MAX(CET) AS tanggal_akhir,
  COUNT(DISTINCT CET) AS tanggal_unik,
  COUNTIF(CloudCover IS NULL) AS cloudcover_kosong
FROM `etl-latihan.cuaca_latihan.data_cuaca`
"""

for row in client.query(query).result():
    print(dict(row))
