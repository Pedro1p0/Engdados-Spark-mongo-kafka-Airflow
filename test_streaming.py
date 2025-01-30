from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaProducer
import json

# Função para enviar uma mensagem de status de conexão para o Kafka
def send_connection_message():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Alterado para o broker correto
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialização de JSON
    )
    message = {
        "status": "Conectado ao Kafka",
        "source": "Spark Streaming",
        "timestamp": "2025-01-30T14:00:00Z"
    }
    producer.send('status_conexao', value=message)  # Enviando para o tópico 'status_conexao'
    producer.flush()  # Garantir que a mensagem seja enviada

# Função para enviar dados processados para o Kafka
def send_processed_data(data):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('dados_clima_processados', value=data)  # Enviando para o tópico 'dados_clima_processados'
    producer.flush()

# Criando uma sessão Spark com suporte ao Kafka
print("Iniciando a sessão Spark...")
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.kafka.bootstrap.servers", "localhost:9092") \
    .getOrCreate()

# Enviar uma mensagem de status após a conexão com o Kafka
print("Enviando mensagem de conexão para o Kafka...")
send_connection_message()

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
print("Lendo dados do Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "consultar_clima") \
    .option("startingOffsets", "earliest") \
    .load()

# Exibindo o schema para garantir que a leitura do Kafka está ocorrendo corretamente
print("Schema dos dados lidos:")
df.printSchema()

# Convertendo os dados JSON da mensagem para colunas estruturadas
print("Convertendo os dados recebidos para o formato estruturado...")
df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Função para processar e enviar os dados processados para o Kafka
def process_and_send_data(batch_df, batch_id):
    # Converte o DataFrame para um formato de lista de dicionários
    processed_data = batch_df.collect()  # Coleta os dados
    for row in processed_data:
        data_dict = row.asDict()  # Converte cada linha em dicionário
        send_processed_data(data_dict)  # Envia o dado para o Kafka
    print(f"Dados processados e enviados para o Kafka no batch {batch_id}")

# Exibindo os dados no console em tempo real (usando writeStream)
print("Exibindo os dados recebidos no console...")
df_console = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_and_send_data) \
    .start()

# Mantém o streaming rodando até que seja interrompido manualmente
print("Streaming iniciado. Aguardando novas mensagens...")
df_console.awaitTermination()
