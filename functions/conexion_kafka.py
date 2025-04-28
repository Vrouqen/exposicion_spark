from kafka import KafkaProducer
import time
import pandas

# Conectamos al docker de Kafka
conn = KafkaProducer(bootstrap_servers=['10.5.2.254:9092'])

df = pandas.read_csv(r'C:\Users\dayascaribay\Desktop\exposicion_spark\functions\bebidas_data.csv')

for _, row in df.iterrows():
    message = row.to_json()
    conn.send('DB_Kafka', value=message.encode('utf-8')).get(timeout=5)
    time.sleep(3)

conn.flush()
conn.close()
