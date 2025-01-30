from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Criando uma sessão Spark com suporte ao Kafka
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.kafka.bootstrap.servers", "localhost:9092") \
    .getOrCreate()

# Definição do esquema das mensagens recebidas do Kafka
schema = StructType([
    StructField("cidade", StringType(), True),
    StructField("temperatura", FloatType(), True),
    StructField("descricao", StringType(), True),
    StructField("umidade", FloatType(), True),
    StructField("pressao", FloatType(), True),
    StructField("vento", FloatType(), True)
])

# Lendo o fluxo de dados do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "consultar_clima") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertendo os dados JSON da mensagem para colunas estruturadas
df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Exibindo os dados recebidos no console para provar o consumo
df_console = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Mantém o streaming rodando até que seja interrompido manualmente
df_console.awaitTermination()
