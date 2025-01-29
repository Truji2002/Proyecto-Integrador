from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

BROKER_URL = "localhost:9092"  # Dirección del broker Kafka
TOPIC_NAME = "test-topic"  # Nombre del tópico en Kafka

def create_producer():
    """
    Crea un productor Kafka utilizando confluent_kafka.
    """
    producer_config = {
        "bootstrap.servers": BROKER_URL  # Clave correcta para confluent-kafka
    }
    producer = Producer(producer_config)
    return producer

def create_consumer():
    """
    Crea un consumidor Kafka utilizando confluent_kafka.
    """
    consumer_config = {
        "bootstrap.servers": BROKER_URL,
        "group.id": "test-group",
        "auto.offset.reset": "earliest"  # Consume mensajes desde el principio si es la primera vez
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_NAME])  # Suscribir al tópico
    return consumer

def publish_message(producer, topic, message):
    """
    Publica un mensaje en el tópico especificado.
    """
    try:
        producer.produce(topic, value=message.encode('utf-8'))
        producer.flush()  # Asegura que los mensajes se envíen al broker
        print(f"Mensaje publicado: {message}")
    except KafkaException as e:
        print(f"Error al publicar el mensaje: {e}")

def consume_messages(consumer):
    """
    Consume mensajes de un tópico de Kafka.
    """
    try:
        print("Esperando mensajes...")
        while True:
            message = consumer.poll(timeout=1.0)  # Espera un mensaje con un tiempo límite
            if message is None:  # No hay mensajes disponibles
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partición alcanzado
                    print("Fin de la partición alcanzado.")
                elif message.error():
                    raise KafkaException(message.error())
            else:
                print(f"Mensaje recibido: {message.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("\nConsumo de mensajes interrumpido.")
    finally:
        consumer.close()  # Cierra el consumidor correctamente

if __name__ == "__main__":
    # Crear el productor y publicar un mensaje
    producer = create_producer()
    publish_message(producer, TOPIC_NAME, "Creando usuario Juan Perez")

    # Crear el consumidor y consumir mensajes
    consumer = create_consumer()
    consume_messages(consumer)
