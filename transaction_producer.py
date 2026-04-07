import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Konfigurasi ke Kafka yang jalan di Docker (Gunakan IP 127.0.0.1 untuk menghindari IPv6 issue)
conf = {'bootstrap.servers': "127.0.0.1:9093"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Gagal kirim pesan: {err}')
    else:
        print(f'Transaksi terkirim ke {msg.topic()} [{msg.partition()}]')

def generate_transaction():
    # Simulasi data transaksi fraud detection
    user_ids = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5']
    locations = ['Jakarta', 'Surabaya', 'Medan', 'London', 'New York']
    
    return {
        'transaction_id': f"tx_{int(time.time() * 1000)}",
        'user_id': random.choice(user_ids),
        'amount': round(random.uniform(10.0, 5000.0), 2),
        'location': random.choice(locations),
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("Memulai pengiriman data transaksi ke Kafka (Ctrl+C buat stop)...")

try:
    while True:
        data = generate_transaction()
        # Kirim ke topic bernama 'transactions'
        producer.produce('transactions', 
                         key=data['user_id'], 
                         value=json.dumps(data), 
                         callback=delivery_report)
        producer.flush()
        time.sleep(1) # Kirim tiap 1 detik
except KeyboardInterrupt:
    print("\nBerhenti...")
