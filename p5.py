from configs import kafka_config, topic_names_dict
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import json
import os

# ! ****************************************************************************
# todo 2. Агрегація даних:
# * Зчитайте потік даних, що ви згенерували в першому пункті. За допомогою _Sliding window_, що має довжину 1 хвилину, _sliding_interval_ — 30 секунд, та _watermark duration_ — 10 секунд, знайдіть середню температуру та вологість.


# Пакет, необхідний для читання Kafka зі Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Створення SparkSession
spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.

# * Зчитайте потік даних, що ви згенерували в першому пункті. За допомогою _Sliding window_, що має довжину 1 хвилину, _sliding_interval_ — 30 секунд, та _watermark duration_ — 10 секунд, знайдіть середню температуру та вологість.
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="'
        + kafka_config["username"]
        + '" password="'
        + kafka_config["password"]
        + '";',
    )
    .option("subscribe", topic_names_dict["building_sensors"])
    .option("startingOffsets", "earliest")
    # .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
)


# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON.
json_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("sensor", StringType(), True),
        StructField("value", IntegerType(), True),
    ]
)

# Маніпуляції з даними
clean_df = (
    df.selectExpr(
        "CAST(value AS STRING) AS value_deserialized",
    )
    .drop("key", "value")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn(
        "timestamp",
        from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp"),
    )
    .withColumn("id", col("value_json.id"))
    .withColumn("sensor", col("value_json.sensor"))
    .withColumn("value", col("value_json.value"))
    .drop("value_json", "value_deserialized")
)

# # Виведення даних на екран
# displaying_df = (
#     clean_df.writeStream.trigger(availableNow=True)
#     .outputMode("append")
#     .format("console")
#     .option("checkpointLocation", "/tmp/checkpoints-2")
#     .option("truncate", "false")
#     .start()
#     # .awaitTermination()
# )


# Визначаємо тривалість вікна і інтервал
window_duration = "1 minute"
sliding_duration = "30 seconds"
watermark_duration = "10 seconds"

# Агрегація по вікну для температури та вологості
aggregated_df = (
    clean_df.withWatermark("timestamp", watermark_duration)  # Встановлюємо watermark
    .groupBy(
        window(col("timestamp"), window_duration, sliding_duration),  # Задаємо вікно
        # col("id"),  # Додавати агрегування для кожного датчика окремо
    )
    .agg(
        avg(when(col("sensor") == "temperature", col("value"))).alias(
            "avg_temperature"
        ),
        avg(when(col("sensor") == "humidity", col("value"))).alias("avg_humidity"),
    )
)

# Виведення результатів В консоль
# aggregated_df.writeStream.trigger(availableNow=True).outputMode("update").format(
#     "console"
# ).option("checkpointLocation", "./tmp/checkpoints-aggregated").option(
#     "truncate", "false"
# ).start().awaitTermination()


# ! ****************************************************************************
# todo 3. Знайомство з параметрами алертів:
# * Щоб деплоїти код кожного разу, параметри алертів вказані в файлі:
# * alerts_conditions.csv
# * Файл містить максимальні та мінімальні значення для температури й вологості, повідомлення та код алерту. Значення -999,-999 вказують, що вони не використовується для цього алерту.
# * Подивіться на дані в файлі. Вони мають бути інтуїтивно зрозумілі. Ви маєте зчитати дані з файлу та використати для налаштування алертів.

# * Зчитування файлу з умовами алертів
alerts_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("alerts_conditions.csv")
)

# alerts_df.show()

# ! ****************************************************************************
# todo 4. Побудова визначення алертів:
# * Після того, як ви знайшли середні значення, необхідно встановити, чи підпадають вони під критерії у файлі (підказка: виконайте cross join та фільтрацію).

# * Виконуємо cross join для об'єднання умов алертів із середніми значеннями
joined_df = alerts_df.crossJoin(aggregated_df)
# joined_df.writeStream.trigger(availableNow=True).outputMode("update").format(
#     "console"
# ).option("checkpointLocation", "./tmp/checkpoints-aggregated").option(
#     "truncate", "false"
# ).start().awaitTermination()

# * Перевіряємо умови для кожного алерту
alerts_result_df = joined_df.filter(
    ((col("humidity_min") == -999) | (col("avg_humidity") >= col("humidity_min")))
    & ((col("humidity_max") == -999) | (col("avg_humidity") <= col("humidity_max")))
    & (
        (col("temperature_min") == -999)
        | (col("avg_temperature") >= col("temperature_min"))
    )
    & (
        (col("temperature_max") == -999)
        | (col("avg_temperature") <= col("temperature_max"))
    )
)

# # Виведення результату: які алерти спрацювали
# alerts_result_df.writeStream.trigger(availableNow=True).outputMode("update").format(
#     "console"
# ).option("checkpointLocation", "./tmp/checkpoints-aggregated").option(
#     "truncate", "false"
# ).start().awaitTermination()

# ! ****************************************************************************
# todo 5. Запис даних у Kafka-топік:
# * Отримані алерти запишіть у вихідний Kafka-топік.
# * Приклад повідомлення в Kafka, що є результатом роботи цього коду:


# ? Створення Kafka Producer
def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


# ? Функція відправки повідомлення в Kafka
def send_to_kafka(row):

    producer = get_kafka_producer()  # Створюємо продюсер
    try:
        # Визначаємо, у який топік відправляти
        topic = (
            topic_names_dict["temperature_alerts"]
            if row["code"] in [103, 104]
            else topic_names_dict["humidity_alerts"]
        )

        data = {
            "window": {
                "start": row["window"]["start"].isoformat(),
                "end": row["window"]["end"].isoformat(),
            },
            "t_avg": row["avg_temperature"],
            "h_avg": row["avg_humidity"],
            "code": row["code"],
            "message": row["message"],
            "timestamp": datetime.now().isoformat(),
        }

        # Надсилаємо повідомлення
        producer.send(topic, value=data)
        print(f"Message sent to topic {topic} successfully. Data: {data}")
        producer.flush()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        producer.close()  # Закриваємо продюсер


def process_batch(df, epoch_id):
    rows = df.collect()  # Забираємо всі рядки
    for row in rows:
        send_to_kafka_partition(row.asDict())  # Відправляємо кожен рядок


alerts_result_df.writeStream.foreach(send_to_kafka).start().awaitTermination()
