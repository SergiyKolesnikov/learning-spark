from datetime import datetime, timezone
from time import sleep

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    # Add Kafka-source library.  The version after ":" must be the Kafka version that you use
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .master("local[4]")
    .appName("KafkaTimeProducer")
    .getOrCreate()
)

counter = 0
schema = "`key` string, `value` string"
while True:
    data = [(str(counter), datetime.now(timezone.utc).isoformat())]
    df = spark.createDataFrame(data=data, schema=schema)
    (
        df.write.format("kafka")
        .option(
            "kafka.bootstrap.servers", "localhost:9093,localhost:9094,localhost:9095"
        )
        .option("topic", "timestamps")
        .save()
    )
    print(data)
    counter += 1
    sleep(1)
