from confluent_kafka import Consumer, Producer, KafkaException

BROKER_URL = "localhost:9092"
GROUP_ID = "grupo_sistemas"

# Configuración de consumidor
consumer_config = {
    "bootstrap.servers": BROKER_URL,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}

# Configuración de productor
producer_config = {
    "bootstrap.servers": BROKER_URL
}

producer = Producer(producer_config)

# Crear consumidores para cada sistema
consumer_odoo = Consumer(consumer_config)
consumer_zentyal = Consumer(consumer_config)
consumer_otrs = Consumer(consumer_config)
consumer_nextcloud = Consumer(consumer_config)

# Suscribirse a los tópicos correspondientes
consumer_odoo.subscribe(["usuarios.crear", "usuarios.eliminar"])
consumer_zentyal.subscribe(["usuarios.crear", "usuarios.eliminar"])
consumer_otrs.subscribe(["incidencias.nueva"])
consumer_nextcloud.subscribe(["actas.crear"])

def enviar_respuesta(topic, mensaje):
    """Publica un mensaje de confirmación en Kafka"""
    producer.produce(topic, value=mensaje.encode('utf-8'))
    producer.flush()

def procesar_mensaje(consumer, nombre_sistema):
    """
    Consume los mensajes, simula la acción en el sistema y envía una respuesta.
    """
    while True:
        msg = consumer.poll(1.0)  # Espera mensajes durante 1 segundo
        if msg is None:
            continue
        if msg.error():
            print(f"❌ [{nombre_sistema}] Error en el mensaje: {msg.error()}")
            continue

        mensaje = msg.value().decode('utf-8')
        print(f"✅ [{nombre_sistema}] Mensaje recibido en {msg.topic()}: {mensaje}")

        # Simulación de acción y respuesta
        if msg.topic() == "usuarios.crear":
            print(f"📌 [{nombre_sistema}] Creando usuario en el sistema...")
            enviar_respuesta("respuesta.usuarios", f"{nombre_sistema}: Usuario creado exitosamente")

        elif msg.topic() == "usuarios.eliminar":
            print(f"🛑 [{nombre_sistema}] Eliminando usuario del sistema...")
            enviar_respuesta("respuesta.usuarios", f"{nombre_sistema}: Usuario eliminado correctamente")

        elif msg.topic() == "incidencias.nueva":
            print(f"🔧 [{nombre_sistema}] Registrando incidencia...")
            enviar_respuesta("respuesta.incidencias", f"{nombre_sistema}: Incidencia registrada")

        elif msg.topic() == "actas.crear":
            print(f"📄 [{nombre_sistema}] Generando acta digital...")
            enviar_respuesta("respuesta.actas", f"{nombre_sistema}: Acta digital creada")

if __name__ == "__main__":
    import threading

    # Ejecutar los consumidores en hilos separados
    threads = [
        threading.Thread(target=procesar_mensaje, args=(consumer_odoo, "Odoo ERP")),
        threading.Thread(target=procesar_mensaje, args=(consumer_zentyal, "Zentyal")),
        threading.Thread(target=procesar_mensaje, args=(consumer_otrs, "OTRS ITSM")),
        threading.Thread(target=procesar_mensaje, args=(consumer_nextcloud, "Nextcloud"))
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
