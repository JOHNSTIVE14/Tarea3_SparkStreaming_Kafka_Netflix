from kafka import KafkaProducer
import json
import time
import random

def get_sensor_data():
    return {
        "temperatura": round(random.uniform(20.0, 35.0), 2),
        "humedad": round(random.uniform(30.0, 90.0), 2)
    }

# Conectar al servidor Kafka local
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📡 Enviando datos al topic 'sensor_data'...")

# Enviar datos cada 2 segundos
while True:
    data = get_sensor_data()
    producer.send('sensor_data', value=data)
    print(f"Mensaje enviado: {data}")
    time.sleep(2)
