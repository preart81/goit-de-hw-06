#
# ? Конфігурація Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# ? Визначення топіків
my_name = "Artur"

topic_names_dict = {
    "building_sensors": f"{my_name}_building_sensors",
    "temperature_alerts": f"{my_name}_temperature_alerts",
    "humidity_alerts": f"{my_name}_humidity_alerts",
}

# Температура — це випадкова величина від 25 до 45.
# Вологість — це випадкова величина від 15 до 85.
sensors_cfg = {
    "temperature": {
        "min": 25,
        "max": 45,
        "alert_lo": 0,
        "alert_hi": 40,
        "topic": topic_names_dict["temperature_alerts"],
        "units": "°C",
    },
    "humidity": {
        "min": 15,
        "max": 85,
        "alert_lo": 20,
        "alert_hi": 80,
        "topic": topic_names_dict["humidity_alerts"],
        "units": "%",
    },
}
