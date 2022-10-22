# Spark Streaming
## PySpark + Apache Kafka

This is a example of a how it works the consumer of a Kafka Topic using Spark and Schema Registry. You can schedule this generic_connection on Airflow following this ✨[Repo](https://github.com/pedrogfx/airflow-docker/blob/main/airflow/dags/dag_kafka_avro.py#L78)✨

- Python
- Spark
- Airflow

## Features
- Consume Kafka Topic;
- Turn into a Spark DataFrame;
- Partition data;
- Write in a bucket in parquet file;
- Read from bucket or create tables from it;
