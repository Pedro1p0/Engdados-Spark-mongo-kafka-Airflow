# Engdados: Integração Spark, MongoDB, Kafka e Airflow

Este projeto implementa um pipeline de processamento de dados em tempo real utilizando **Apache Spark Streaming**, **Apache Kafka**, **MongoDB** e **Apache Airflow**. O objetivo é capturar, processar e armazenar dados de consultas climáticas de maneira eficiente.

## Configuração do Ambiente

### 1. Criar e Ativar um Ambiente Virtual

Para garantir a correta instalação das dependências, é recomendado criar e ativar um ambiente virtual Python:

```bash
python -m venv venv  # Criar ambiente virtual
source venv/bin/activate  # Ativar no Linux/macOS
venv\Scripts\activate  # Ativar no Windows
```

### 2. Instalar Dependências

Com o ambiente virtual ativado, instale as dependências listadas no `requirements.txt`:

```bash
pip install -r requirements.txt
```

Faça o build do docker compose, caso os containers não subam de primeira, tente subir novamente.

### 3. Criar o Tópico no Kafka

Acesse a interface do **Confluent Kafka** via navegador em `http://localhost:9021` e crie um tópico chamado:

```
consultar_clima
```

Isso garantirá que os dados serão enviados corretamente ao Kafka e consumidos pelo Spark Streaming.

### 4. Executar o Spark Streaming

Para iniciar o processo de ingestão e processamento dos dados em tempo real, execute o seguinte comando:

```bash
spark-submit --master spark://localhost:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
    test_streaming.py
```

### 5. Monitoramento

- Acompanhe a chegada das mensagens na interface do **Confluent Kafka** (`http://localhost:9021`).
- Verifique a comunicação entre `test_streaming.py`, o **MongoDB** e o **Kafka** analisando os logs.
- Utilize o **Apache Airflow** para gerenciar e monitorar os processos de ETL.

---

## Tecnologias Utilizadas

- **Apache Kafka**: Sistema de mensagens para transmissão de eventos em tempo real.
- **Apache Spark Streaming**: Framework de processamento de dados em tempo real.
- **MongoDB**: Banco de dados NoSQL para armazenamento dos resultados.
- **Apache Airflow**: Orquestração e automação do pipeline de dados.
- **Docker & Docker-Compose**: Gerenciamento de containers para os serviços.

Este projeto oferece uma solução robusta para análise e processamento de dados em tempo real. Para mais detalhes sobre a implementação, consulte os arquivos do repositório.
