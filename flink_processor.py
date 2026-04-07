from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common import Configuration
import os
import redis

def run_flink_job():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)

    t_env.get_config().set("table.exec.resource.default-parallelism", "1")
    t_env.get_config().set("pipeline.default-parallelism", "1")

    curr_dir = os.path.dirname(os.path.realpath(__file__))
    jar_path = f"file:///{os.path.join(curr_dir, 'lib', 'flink-sql-connector-kafka-3.1.0-1.18.jar')}".replace("\\", "/")
    
    t_env.get_config().set("pipeline.jars", jar_path)
    t_env.get_config().set("classloader.check-leaked-classloader", "false")

    t_env.execute_sql("""
        CREATE TABLE kafka_transactions (
            user_id STRING,
            amount DOUBLE,
            location STRING,
            `timestamp` STRING,
            ts AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'localhost:9093',
            'properties.group.id' = 'fraud-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    res_table = t_env.sql_query("""
        SELECT 
            user_id, 
            SUM(amount) as total_spending_1min,
            COUNT(user_id) as tx_count_1min
        FROM TABLE(
            TUMBLE(TABLE kafka_transactions, DESCRIPTOR(ts), INTERVAL '1' MINUTES))
        GROUP BY user_id, window_start, window_end
    """)

    print("Memulai Flink Job... Proses Menulis ke Redis berjalan...")
    
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    table_result = res_table.execute()

    with table_result.collect() as results:
        for row in results:
            user_id = row[0]
            total_spending = row[1]
            tx_count = row[2]

            redis_key = f"user_features:{user_id}"

            try:
                redis_client.hset(redis_key, mapping={
                    "total_spending_1min": float(total_spending),
                    "tx_count_1min": int(tx_count),
                })

                redis_client.expire(redis_key, 300)

                print(f"[REDIS SUCCESS] Diperbarui {redis_key} -> Total: {total_spending:.2f} | Count: {tx_count}")
            except Exception as e:
                print(f"[REDIS ERROR] Gagal menyimpan data untuk {user_id}: {e}")

if __name__ == '__main__':
    run_flink_job()
