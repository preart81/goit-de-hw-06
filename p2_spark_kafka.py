from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config, topic_names_dict
import os

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
aggregated_df.writeStream.trigger(availableNow=True).outputMode("update").format(
    "console"
).option("checkpointLocation", "./tmp/checkpoints-aggregated").option(
    "truncate", "false"
).start().awaitTermination()
