from kafka import KafkaProducer
import time
import pandas

# Conectamos al docker de Kafka
conn = KafkaProducer(bootstrap_servers=['192.168.200.12:9092'])

df = pandas.read_csv(r'C:\Users\luise\Desktop\Mineria\Practicas\Ejemplo_practico_spark\functions\bebidas_data.csv')

for _, row in df.iterrows():
    message = row.to_json()
    conn.send('DB_Kafka', value=message.encode('utf-8')).get(timeout=5)
    time.sleep(3)

conn.flush()
conn.close()
