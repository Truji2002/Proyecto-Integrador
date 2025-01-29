from confluent_kafka import Consumer

BROKER_URL = "localhost:9092"
GROUP_ID = "grupo_respuestas"

# Configuración del consumidor de respuestas
consumer_config = {
    "bootstrap.servers": BROKER_URL,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

# Suscribirse a los tópicos de respuestas
consumer.subscribe(["respuesta.usuarios", "respuesta.incidencias", "respuesta.actas"])

print("📢 Esperando respuestas del sistema...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"❌ Error en la respuesta: {msg.error()}")
        continue

    print(f"📨 Respuesta recibida: {msg.value().decode('utf-8')}")
