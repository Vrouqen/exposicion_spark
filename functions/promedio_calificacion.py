from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql import functions 
import logging

# Creación de la sesión de Spark
s_session = SparkSession.builder \
    .appName("PromedioSaborPorMarca") \
    .getOrCreate()

# Reducir la verbosidad de los logs
s_session.sparkContext.setLogLevel("WARN")
logging.getLogger("py4j").setLevel(logging.ERROR)

# Lectura del stream desde Kafka
df_kafka = s_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.5.2.254:9092") \
    .option("subscribe", "DB_Kafka") \
    .load()

# Extraemos el valor (mensaje) y lo convertimos en formato JSON
df_kafka = df_kafka.select(
    col("value").cast("string").alias("json_data")
)

# Importamos librerías para manipular los JSONs
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Definimos el esquema de los datos (basado en el CSV original)
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

# Convertimos el JSON a un DataFrame estructurado
df = df_kafka.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# Ahora calculamos el promedio del sabor por marca
df_avg_sabor = df.groupBy("MARCA").agg(
    functions.round(avg("SABOR"), 2).alias("PROM_SABOR")
)

# Mostrar los resultados en la consola
query = df_avg_sabor.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
