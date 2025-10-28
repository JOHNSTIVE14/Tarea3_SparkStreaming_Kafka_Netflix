from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType

# Crear la sesión Spark con soporte para Kafka
spark = SparkSession.builder \
    .appName("SparkStreamingKafkaConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los mensajes que envía el productor
schema = StructType([
    StructField("temperatura", DoubleType()),
    StructField("humedad", DoubleType())
])

# Leer datos en tiempo real desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Extraer y convertir los datos JSON
values = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Mostrar los datos en la consola
query = values.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()


