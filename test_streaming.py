import logging
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_mongo_connection():
    try:
        # Conectando ao MongoDB Atlas
        client = MongoClient("mongodb+srv://pedroclemente119:ISCigIWBU3AmVb4Q@mongopyshark.b2mno.mongodb.net/?retryWrites=true&w=majority&appName=MongoPyShark")
        db = client['spark_streams']
        return db
    except Exception as e:
        logging.error(f"Could not create MongoDB connection due to {e}")
        return None

def create_collection(db):
    try:
        # Verifica se a coleção já existe antes de tentar criá-la
        if 'created_users' not in db.list_collection_names():
            db.create_collection('created_users')
            print("Collection created successfully!")
        else:
            print("Collection already exists, proceeding...")
    except Exception as e:
        print(f"Error checking/creating collection: {e}")

def insert_data(db, **kwargs):
    print("Inserting data...")
    
    user_data = {key: kwargs.get(key) for key in ['id', 'name', 'email']}
    try:
        db['created_users'].insert_one(user_data)
        logging.info(f"Data inserted for {user_data['name']} {user_data['email']}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:10.0.0," \
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('group.id', 'spark-consumer-group')\
            .option('startingOffsets', 'earliest') \
            .load()

        print(spark_df.isStreaming)

        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    if spark_df is None:
        logging.error("Input DataFrame is None, unable to create selection DataFrame")
        return None

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
    ])

    try:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        logging.info("Selection DataFrame created successfully")
        return sel
    except Exception as e:
        logging.error(f"Error creating selection DataFrame: {e}")
        return None

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        if selection_df:
            db = create_mongo_connection()
            if db is not None:
                create_collection(db)
                db['created_users'].insert_one({'id': '123', 'name': 'Test User', 'email': 'test@example.com'})
                print("Teste de inserção realizado com sucesso!")
                logging.info("Streaming is being started...")
                try:
                    streaming_query = (selection_df.writeStream
                        .format("mongodb")
                        .option("checkpointLocation", "/tmp/checkpoint")
                        .option("writeConcern.w", "majority")
                        .option("mongo.batchSize", "1000")
                        .option("uri", "mongodb+srv://pedroclemente119:ISCigIWBU3AmVb4Q@mongopyshark.b2mno.mongodb.net/mongopyshark")
                        .start())
                    # Teste de apenas streaming para o console
                    if selection_df:
                        checkpoint_location = "/tmp/checkpoint"
                        query = selection_df.writeStream \
                            .outputMode("append") \
                            .format("console") \
                            .option("checkpointLocation", checkpoint_location) \
                            .start()
                        try:
                            query.awaitTermination()
                        except Exception as e:
                            logging.error(f"Streaming terminated with error: {e}")
                except Exception as e:
                    logging.error(f"Error during streaming: {e}")
            else:
                logging.error("MongoDB connection could not be established.")
        else:
            logging.error("Selection DataFrame is None, unable to proceed with streaming.")
    else:
        logging.error("Spark connection could not be created.")