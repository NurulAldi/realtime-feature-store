import os
import json
import pyarrow as pa
from confluent_kafka import Consumer
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType
from pathlib import Path
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

warehouse_folder = "warehouse"
os.makedirs(warehouse_folder, exist_ok=True)

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_folder}/pyiceberg_catalog.db",
        "warehouse": warehouse_folder
    }
)

catalog.create_namespace_if_not_exists("default")

iceberg_schema = Schema(
    NestedField(field_id=1, name="transaction_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="user_id", field_type=StringType(), required=False),
    NestedField(field_id=3, name="amount", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="location", field_type=StringType(), required=False),
    NestedField(field_id=5, name="timestamp", field_type=StringType(), required=False),
)

arrow_schema = pa.schema([
    ('transaction_id', pa.string()),
    ('user_id', pa.string()),
    ('amount', pa.float64()),
    ('location', pa.string()),
    ('timestamp', pa.string()),
])

partition_spec = PartitionSpec(
    PartitionField(
        source_id=4,
        field_id=1000,
        transform=IdentityTransform(),
        name="location"
    )
)

table_name = "default.transactions_offline"
try:
    table = catalog.load_table(table_name)
    print(f"Tabel {table_name} ditemukan.")
except:
    print(f"Tabel {table_name} belum ada, membuat baru dengan partisi 'location'...")
    table = catalog.create_table(identifier=table_name, schema=iceberg_schema, partition_spec=partition_spec)

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9093',
    'group.id': 'iceberg-ingest-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['transactions'])

print("Memulai Ingestor Kafka ke Iceberg...")

buffer = []
BATCH_SIZE = 1000 # Setiap 1000 transaksi, commit menjadi 1 file Parquet di Iceberg

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error Consumer: {msg.error()}")
            continue
        
        record = json.loads(msg.value().decode('utf-8'))
        buffer.append(record)

        if len(buffer) >= BATCH_SIZE:
            df_arrow = pa.Table.from_pylist(buffer, schema=arrow_schema)

            table.append(df_arrow)
            print(f"Berhasil commit {len(buffer)} data ke tabel Iceberg!")

            buffer.clear()
except KeyboardInterrupt:
    print("\nProses dihentikan oleh user.")
finally:
    if len(buffer) > 0:
        df_arrow = pa.Table.from_pylist(buffer, schema=arrow_schema)
        table.append(df_arrow)
        print(f"Commit sisa {len(buffer)} data ditarik sebelum dimatikan.")
    consumer.close()