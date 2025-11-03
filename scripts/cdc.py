# -*- coding: utf-8 -*-
"""
Script: cdc_pgoutput.py
Descrição:
  Consome eventos CDC em modo replicação lógica (pgoutput)
  e envia os frames capturados para o MinIO.
"""

from minio import Minio
from io import BytesIO
from datetime import datetime
from psycopg2.extras import LogicalReplicationConnection
import psycopg2, json

# === CONFIGURAÇÕES ===
PG_HOST = "db"
PG_PORT = 5432
PG_DATABASE = "mydb"
PG_USER = "myuser"
PG_PASSWORD = "mypassword"

REPLICATION_SLOT = "data_sync_slot"
PUBLICATION_NAME = "data_sync_pub"
SCHEMA = "db_loja"
TABLE = "cliente"

BUCKET_NAME = "raw"
DEST_PREFIX = "cdc/cliente"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
SECURE_CONNECTION = False


def ensure_cdc_ready():
    """Garante que publicação e slot existam."""
    print("🧩 Verificando (ou criando) publicação e replication slot...")

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Publicação
    cur.execute(f"SELECT 1 FROM pg_publication WHERE pubname = '{PUBLICATION_NAME}';")
    if not cur.fetchone():
        cur.execute(f"CREATE PUBLICATION {PUBLICATION_NAME} FOR TABLE {SCHEMA}.{TABLE};")
        print(f"✅ Publicação '{PUBLICATION_NAME}' criada.")
    else:
        print(f"✅ Publicação '{PUBLICATION_NAME}' já existe.")

    # Slot
    cur.execute(f"SELECT 1 FROM pg_replication_slots WHERE slot_name = '{REPLICATION_SLOT}';")
    if not cur.fetchone():
        cur.execute(f"SELECT pg_create_logical_replication_slot('{REPLICATION_SLOT}', 'pgoutput');")
        print(f"✅ Slot '{REPLICATION_SLOT}' criado com plugin pgoutput.")
    else:
        print(f"✅ Slot '{REPLICATION_SLOT}' já existe.")

    cur.close()
    conn.close()


def capture_pgoutput():
    """Captura os frames CDC do slot usando LogicalReplicationConnection."""
    print("🔗 Iniciando streaming lógico do slot...")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
        connection_factory=LogicalReplicationConnection
    )
    cur = conn.cursor()

    events = []

    def consume(msg):
        decoded = msg.payload.decode("utf-8", errors="ignore")
        events.append({
            "time": datetime.now().isoformat(),
            "payload": decoded
        })
        print(f"📦 Evento CDC recebido: {decoded[:80]}...")
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    cur.start_replication(
        slot_name=REPLICATION_SLOT,
        options={"proto_version": "1", "publication_names": PUBLICATION_NAME},
        decode=True
    )
    cur.consume_stream(consume, keepalive_interval=10)

    cur.close()
    conn.close()
    return events


def upload_to_minio(events):
    """Salva os eventos em JSON no MinIO."""
    if not events:
        print("⚠️ Nenhum evento recebido do slot.")
        return

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=SECURE_CONNECTION
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")

    date_str = datetime.now().strftime("%Y%m%d")
    time_str = datetime.now().strftime("%H%M%S")
    object_name = f"{DEST_PREFIX}/pgoutput_cdc_{date_str}_{time_str}.json"

    json_bytes = json.dumps(events, indent=2, ensure_ascii=False).encode("utf-8")
    buffer = BytesIO(json_bytes)

    client.put_object(
        BUCKET_NAME,
        object_name,
        buffer,
        length=len(buffer.getvalue()),
        content_type="application/json"
    )

    print(f"✅ Upload concluído: s3://{BUCKET_NAME}/{object_name}")


def main():
    print("🚀 Captura CDC via pgoutput iniciada...")
    ensure_cdc_ready()
    events = capture_pgoutput()
    upload_to_minio(events)


if __name__ == "__main__":
    main()
