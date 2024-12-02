from kafka import KafkaProducer
from configs import kafka_config, topic_names_dict, sensors_cfg

import json
import uuid
import time
import random

# todo 2. Відправка даних до топіків:
# * Напишіть Python-скрипт, який імітує роботу датчика і періодично відправляє випадково згенеровані дані (температура та вологість) у топік building_sensors.
# * Дані мають містити ідентифікатор датчика, час отримання даних та відповідні показники.
# * Один запуск скрипту має відповідати тільки одному датчику. Тобто, для того, щоб імітувати декілька датчиків, необхідно запустити скрипт декілька разів.
# * ID датчика може просто бути випадковим числом, але постійним (однаковим) для одного запуску скрипту. При повторному запуску ID датчика може змінюватись.
# * Температура — це випадкова величина від 25 до 45.
# * Вологість — це випадкова величина від 15 до 85.


# ? Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def send_message():

    # ? Вибираємо рандомний датчик
    sensors = ["temperature", "humidity"]
    sensor = random.choice(sensors)
    # print(f"{sensor = }")

    # ? Вибираємо рандомне значення в заданих діапазонах
    s_min, s_max = sensors_cfg[sensor]["min"], sensors_cfg[sensor]["max"]
    sensor_value = random.randint(s_min, s_max)
    sensor_id = random.randint(1, 10)
    # print(f"{sensor}[{s_min}-{s_max}]={sensor_value}")

    # ? Відправлення повідомлення в топік
    try:
        topic_name = topic_names_dict["building_sensors"]
        data = {
            "timestamp": time.time(),
            "id": sensor_id,
            "sensor": sensor,
            "value": sensor_value,
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message sent to topic '{topic_name}' successfully. Data: {data}")
    except Exception as e:
        print(f"An error occurred: {e}")


# ? Відправка повідомлень в циклі
for i in range(20):
    send_message()
    time.sleep(0.1)

producer.close()  # Закриття producer
