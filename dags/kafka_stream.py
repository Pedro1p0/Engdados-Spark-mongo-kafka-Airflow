import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import time
import logging
import requests
from kafka import KafkaProducer

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# Definir parâmetros da API do clima
API_KEY = 'b3623be5384e123fc5bba09640cb3ff4'
CITY = 'Natal,RN'
BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"
URL = BASE_URL + "appid=" + API_KEY + "&q=" + CITY + "&units=metric"

# Função para consultar o clima
def consultar_clima():
    try:
        response = requests.get(URL)
        data = response.json()

        if data['cod'] != 200:
            logging.error(f"Erro ao consultar clima: {data['message']}")
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
        logging.error(f"Erro ao fazer a requisição para a API: {e}")
        return None

# Função para enviar dados ao Kafka
def enviar_para_kafka(**kwargs):
    producer = KafkaProducer(bootstrap_servers=['broker:29092'])
    curr_time = time.time()
    message = None

    while time.time() < curr_time + 60:  # Limita o tempo de execução a 1 minuto
        try:
            clima = consultar_clima()
            if clima:
                # Envia a mensagem para o Kafka
                future = producer.send('consultar_clima', json.dumps(clima).encode('utf-8'))
                record_metadata = future.get(timeout=10)  # Espera até 10 segundos por confirmação
                message = f"Mensagem enviada para o tópico 'consultar_clima': {record_metadata}"
                print(message)
                time.sleep(1)  # Aguarda 1 segundo entre envios
            else:
                logging.error("Nenhum dado válido obtido da API.")
        except Exception as e:
            message = f"Erro ao enviar dados: {e}"
            logging.error(message)
            continue

    # Usando XCom para enviar a mensagem de volta
    kwargs['ti'].xcom_push(key='message', value=message)

# Função para verificar a mensagem enviada
def check_message(**kwargs):
    message = kwargs['ti'].xcom_pull(task_ids='stream_data_from_api', key='message')
    print(f"Mensagem retornada: {message}")

# Definindo a DAG
with DAG('clima_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Tarefa para enviar os dados para o Kafka
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=enviar_para_kafka,
        provide_context=True  # Permite o acesso ao contexto do Airflow (XCom)
    )

    # Tarefa para verificar a mensagem enviada
    check_message_task = PythonOperator(
        task_id='check_message',
        python_callable=check_message,
        provide_context=True  # Permite o acesso ao contexto do Airflow
    )

    # Definir a ordem de execução das tarefas
    streaming_task >> check_message_task
