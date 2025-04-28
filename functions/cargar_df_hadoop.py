from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import logging

# Creación de la sesión de Spark
s_session = SparkSession.builder \
    .appName("GuardarCSV_HDFS") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Reducir la verbosidad de los logs
s_session.sparkContext.setLogLevel("WARN")
logging.getLogger("py4j").setLevel(logging.ERROR)

# Lectura del stream desde Kafka
df_kafka = s_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.200.12:9092") \
    .option("subscribe", "DB_Kafka") \
    .option("startingOffsets", "earliest") \
    .load()

# Extraemos el valor (mensaje) y lo convertimos en string
df_kafka = df_kafka.select(
    col("value").cast("string").alias("json_data")
)

# Definimos el esquema (igual que el CSV original)
schema = StructType([
    StructField("MARCA", StringType(), True),
    StructField("GÉNERO", StringType(), True),
    StructField("EDAD", StringType(), True),
    StructField("SABOR", FloatType(), True),
    StructField("INGREDIENTES", FloatType(), True),
    StructField("PUBLICIDAD", FloatType(), True),
    StructField("DISEÑO", FloatType(), True),
    StructField("ESTATUS", FloatType(), True),
    StructField("PREFERENCIA", FloatType(), True),
    StructField("PREMIUM", FloatType(), True),
    StructField("ACCESIBILIDAD", FloatType(), True),
    StructField("JUVENTUD", FloatType(), True)
])

# Convertimos el JSON a columnas estructuradas
df = df_kafka.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# Guardamos el DataFrame directamente en HDFS como CSV
query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/user/spark/output/datos_csv") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoint/datos_csv") \
    .option("header", "true") \
    .start()

query.awaitTermination()
