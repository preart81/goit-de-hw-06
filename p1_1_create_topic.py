from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, topic_names_dict, my_name

# todo 1. Генерація потоку даних:
# * Вхідні дані — це дані з Kafka-топіку, такі самі, як і в попередньому
# * домашньому завданні. Згенеруйте потік даних, що містить id, temperature,
# * humidity, timestamp. Можна використати раніше написаний вами скрипт та
# * топік.

# * 1. Створення топіків в Kafka:
# * Створіть три топіки в Kafka:
# * До імен топіків добавте свої імена або інші ідентифікатори, щоб імена топіків не дублювалися.
# * building_sensors — для зберігання даних з усіх датчиків,
# * temperature_alerts — для зберігання сповіщень про перевищення допустимого рівня температури,
# * humidity_alerts — для зберігання сповіщень про вихід рівня вологості за допустимі рамки.


# ? Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)


# ? Визначення нового топіку
topic_names_list = list(topic_names_dict.values())
num_partitions = 2
replication_factor = 1


# ? Видалення топіків
print(f"\nDeleting topics:")
for topic in admin_client.list_topics():
    if topic.startswith(f"{my_name}_"):
        try:
            admin_client.delete_topics([topic])
            print(f"Topic '{topic}' deleted successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")


for topic in admin_client.list_topics():
    if my_name in topic:
        print(topic)

# ? Створення топіків
print(f"\nCreating topics: {topic_names_list}")
for topic_name in topic_names_list:
    new_topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    # ? Створення топіку
    try:
        res = admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


# ? Перевіряємо список існуючих топіків

topics_on_kafka = admin_client.list_topics()

# # * вивести всі топіки
# print(f"\nTopics on Kafka:")
# print("\n".join(sorted(topics_on_kafka)))

# * вивести тільки топіки, які ми створили
print(f"\nOur topics on Kafka:")
for topic in admin_client.list_topics():
    if topic.startswith(f"{my_name}_"):
        print(topic)


# ? Закриття зв'язку з клієнтом
admin_client.close()
