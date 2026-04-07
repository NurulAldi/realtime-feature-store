import redis
import time
import random

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

MAX_SPENDING_1MIN = 10000.0
MAX_TX_COUNT_1MIN = 5

def check_fraud(user_id, current_amount):
    redis_key = f"user_features:{user_id}"

    features = redis_client.hgetall(redis_key)

    total_spending = float(features.get("total_spending_1min", 0.0))
    tx_count = int(features.get("tx_count_1min", 0))

    if tx_count >= MAX_TX_COUNT_1MIN:
        return "REJECT", f"Terlalu banyak transaksi dalam 1 menit ({tx_count} kali)"
    
    if (total_spending + current_amount) > MAX_SPENDING_1MIN:
        return "REJECT", f"Melebihi limit pengeluaran (Total sebelumnya: {total_spending:.2f} + {current_amount:.2f})"
    
    return "APPROVE", "Transaksi wajar"

def simulate_realtime_decisions():
    user_ids = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5']

    print("Memulai Fraud Detection System (Ctrll+c untuk stop)...")
    print("-" * 60)

    try:
        while True:
            user_id = random.choice(user_ids)
            current_amount = round(random.uniform(50.0, 3000.0), 2)

            print(f"Incoming TX: {user_id} mencoba transaksi sebesar ${current_amount}")

            decision, reason = check_fraud(user_id, current_amount)

            if decision == "APPROVE":
                print(f"[APPROVE] {user_id}, Alasan: {reason}")
            else:
                print(f"[REJECT] {user_id}, Alasan: {reason}")

            print("-" * 60)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nSistem Fraud Detection dihentikan...")

if __name__ == "__main__":
    simulate_realtime_decisions()