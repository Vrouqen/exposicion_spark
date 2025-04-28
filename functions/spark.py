from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Creación de la sesión de Spark
s_session = SparkSession.builder \
    .appName("Carga_df") \
    .getOrCreate()

# Lectura del stream desde Kafka
df_anemia = s_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.200.12:9092") \
    .option("subscribe", "DB_Kafka") \
    .load()

df_anemia = df_anemia.select(
    col("value").cast("string").alias("row")
)

leer = df_anemia.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

leer.awaitTermination()
