import requests
from kafka import KafkaProducer
import json
import time

# Configurações
API_KEY = 'b3623be5384e123fc5bba09640cb3ff4'
CITY = 'Natal,RN'
KAFKA_TOPIC = 'consultar_clima'
KAFKA_SERVER = 'localhost:9092'

# URL da API OpenWeatherMap
# URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric&lang=pt'
API_KEY = 'b3623be5384e123fc5bba09640cb3ff4'
CITY = "NATAL"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"
URL = BASE_URL + "appid=" + API_KEY + "&q=" + CITY + "&units=metric"
# Função para consultar o clima
def consultar_clima():
    try:
        response = requests.get(URL)
        data = response.json()

        if data['cod'] != 200:
            print(f"Erro ao consultar clima: {data['message']}")
            return None

        clima = {
            'cidade': CITY,
            'temperatura': data['main']['temp'],
            'descricao': data['weather'][0]['description'],
            'umidade': data['main']['humidity'],
            'pressao': data['main']['pressure'],
            'vento': data['wind']['speed']
        }

        return clima

    except Exception as e:
        print(f"Erro ao fazer a requisição para a API: {e}")
        return None


# Função para publicar no Kafka
def enviar_para_kafka(clima):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        producer.send(KAFKA_TOPIC, clima)
        producer.flush()
        print(f'Clima enviado para o Kafka: {clima}')
    except Exception as e:
        print(f"Erro ao enviar para o Kafka: {e}")
    finally:
        producer.close()


# Loop para consultar o clima periodicamente e enviar para o Kafka
while True:
    clima = consultar_clima()

    if clima:
        enviar_para_kafka(clima)

    time.sleep(60)
