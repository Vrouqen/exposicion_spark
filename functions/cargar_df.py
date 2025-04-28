from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Creación de la sesión de Spark
s_session = SparkSession.builder \
    .appName("Carga_df") \
    .getOrCreate()

# Reducir la verbosidad de los logs
s_session.sparkContext.setLogLevel("WARN")
logging.getLogger("py4j").setLevel(logging.ERROR)

# Lectura del stream desde Kafka
df_bebidas = s_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.200.12:9092") \
    .option("subscribe", "DB_Kafka") \
    .load()

df_bebidas = df_bebidas.select(
    col("value").cast("string").alias("row")
)

leer = df_bebidas.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

leer.awaitTermination()