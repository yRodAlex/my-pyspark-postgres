# -*- coding: utf-8 -*-
"""
Captura incremental (CDC) de alterações via replication slot do PostgreSQL
e grava os dados no bucket MinIO (raw/cdc) em formato CSV.
"""

import psycopg2
import pandas as pd
from datetime import datetime
from io import BytesIO
from minio import Minio
import os

# === CONFIGURAÇÕES ===
PG_HOST = "db"
PG_PORT = 5432
PG_DATABASE = "mydb"
PG_USER = "myuser"
PG_PASSWORD = "mypassword"
REPLICATION_SLOT = "data_sync_slot"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw"
PREFIX = "cdc"
SECURE_CONNECTION = False

LSN_FILE = "last_lsn.txt"   # marcador de página local

# === FUNÇÃO AUXILIAR ===
def get_last_lsn():
    if os.path.exists(LSN_FILE):
        with open(LSN_FILE, "r") as f:
            return f.read().strip()
    return None

def save_last_lsn(lsn):
    with open(LSN_FILE, "w") as f:
        f.write(lsn)

# === CONECTAR AO POSTGRES ===
print("🔗 Conectando ao PostgreSQL para capturar alterações CDC...")
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD
)
cur = conn.cursor()

# === RECUPERA LSN INICIAL ===
last_lsn = get_last_lsn()
if last_lsn:
    query = f"SELECT * FROM pg_logical_slot_get_changes('{REPLICATION_SLOT}', '{last_lsn}', NULL);"
else:
    query = f"SELECT * FROM pg_logical_slot_get_changes('{REPLICATION_SLOT}', NULL, NULL);"

print(f"📡 Executando CDC a partir de LSN: {last_lsn or 'início do slot'}")
cur.execute(query)
changes = cur.fetchall()

if not changes:
    print("⚠️ Nenhuma mudança encontrada no slot.")
    cur.close()
    conn.close()
    exit()

# === MONTA DATAFRAME ===
rows = []
for change in changes:
    lsn, xid, data = change
    rows.append({"lsn": lsn, "xid": xid, "data": data})
    last_lsn = lsn

df = pd.DataFrame(rows)
print(f"📊 {len(df)} alterações capturadas.")

# === SALVA CSV EM MEMÓRIA ===
csv_buffer = BytesIO()
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

# === CONECTAR AO MINIO ===
print("🚀 Conectando ao MinIO...")
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=SECURE_CONNECTION
)

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"🪣 Bucket '{BUCKET_NAME}' criado.")

# === ENVIO DO ARQUIVO CSV ===
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
object_name = f"{PREFIX}/cdc_changes_{timestamp_str}.csv"

client.put_object(
    BUCKET_NAME,
    object_name,
    csv_buffer,
    length=len(csv_buffer.getvalue()),
    content_type="text/csv"
)

print(f"✅ Arquivo enviado: s3://{BUCKET_NAME}/{object_name}")

# === ATUALIZA MARCADOR DE PÁGINA ===
if last_lsn:
    save_last_lsn(last_lsn)
    print(f"💾 Último LSN salvo: {last_lsn}")

# === FINALIZA ===
cur.close()
conn.close()
print("🏁 CDC concluído com sucesso.")
