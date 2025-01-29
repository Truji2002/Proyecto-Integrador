from flask import Flask, request, jsonify
from confluent_kafka import Producer

app = Flask(__name__)

BROKER_URL = "localhost:9092"
producer = Producer({"bootstrap.servers": BROKER_URL})

@app.route("/usuarios/crear", methods=["POST"])
def crear_usuario():
    data = request.json
    # Publicar evento en Kafka
    producer.produce("usuarios.crear", value=str(data))
    producer.flush()
    return jsonify({"status": "Usuario creado", "data": data}), 201

@app.route("/usuarios/eliminar", methods=["POST"])
def eliminar_usuario():
    data = request.json
    # Publicar evento en Kafka
    producer.produce("usuarios.eliminar", value=str(data))
    producer.flush()
    return jsonify({"status": "Usuario eliminado", "data": data}), 201

@app.route("/incidencias/nueva", methods=["POST"])
def nueva_incidencia():
    data = request.json
    # Publicar evento en Kafka
    producer.produce("incidencias.nueva", value=str(data))
    producer.flush()
    return jsonify({"status": "Incidencia registrada", "data": data}), 201

@app.route("/actas/crear", methods=["POST"])
def crear_acta():
    data = request.json
    # Publicar evento en Kafka
    producer.produce("actas.crear", value=str(data))
    producer.flush()
    return jsonify({"status": "Acta creada", "data": data}), 201

if __name__ == "__main__":
    app.run(port=5000)
