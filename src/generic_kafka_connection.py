from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import sys
from pyspark.sql.avro.functions import from_avro
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

SCALA_VERSION = "2.12"
SPARK_VERSION = "3.1.2"

spark = SparkSession.builder.appName('KAFKA_AVRO_APP') \
    .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION},org.apache.spark:spark-streaming-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION},org.apache.spark:spark-avro_2.12:3.1.2')\
    .getOrCreate()

schema_str = sys.argv[1]
EH_SASL = sys.argv[2]
table_id = sys.argv[3]
project_id = sys.argv[4]
bucket_id = sys.argv[5]
layer_dataset = sys.argv[6]
vmode = sys.argv[7]
vgroup_id = sys.argv[8]
topic = sys.argv[9]
bucket = sys.argv[10]
SECURITY = sys.argv[11]
BOOT = sys.argv[12]

fromAvroOptions = {"mode": "PERMISSIVE"}
schema_registry = SchemaRegistryClient({"url": "url_schema",
                                        "basic.auth.user.info": "api_key_auth"})
schema_str = schema_registry.get_latest_version(
    f'{topic}' + '-value').schema.schema_str
new_schema = json.loads(schema_str)

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOT) \
    .option("kafka.security.protocol", SECURITY) \
    .option("kafka.sasl.jaas.config", EH_SASL) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("subscribe", f'{topic}') \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("inferSchema", True)\
    .option("header", True)\
    .load()\
    .select(F.substring(F.col("value"), 6, 100000).alias("value"))\

df2 = df.select(from_avro(F.col("value"), schema_str,
                fromAvroOptions).alias("value"))

df3 = df2.select('value.metadata.eventName', 'value.payload.*')

query = df3.repartition(1)\
    .writeStream\
    .format("parquet")\
    .outputMode("append")\
    .option("parentProject", project_id) \
    .option("checkpointLocation", bucket + table_id + "_checkpoint")\
    .trigger(processingTime='600 seconds')\
    .start(bucket + table_id + "_file")

# READ THE PARQUET FILE ON BUCKET
vpath = bucket + table_id + '_file/part*'
dfp = spark.read.format("parquet").option("header", "true").load(vpath)
dfp.createOrReplaceTempView("tmp")
tmp = spark.sql('SELECT * FROM tmp')


def write_gcs(df, mode, path, partition):
    # if partition.lower() in (partition.lower() for partition in df.columns):
    if 'DAT_TABLE'.lower() in (partition.lower() for partition in df.columns):
        df = df.withColumn(partition, F.to_date(partition).cast('date'))
    else:
        partition = 'DAT_TABLE'
        df = df.withColumn(partition, F.current_date())
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode(mode).partitionBy(partition).parquet(path)
    return df


def create_table_external_hive_partitioned(table_id, path, project):
    original_table_id = table_id
    # Example file:
    # gs://cloud-samples-data/bigquery/hive-partitioning-samples/autolayout/dt=2020-11-15/file1.parquet
    uri = path+"*"
    # TODO(developer): Set source uri prefix.
    source_uri_prefix = (
        path
    )
    # [END bigquery_create_table_external_hivepartitioned]
    table_id = original_table_id
    # [START bigquery_create_table_external_hivepartitioned]
    from google.cloud import bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project)
    # Configure the external data source.
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [uri]
    external_config.autodetect = True
    # Configure partitioning options.
    hive_partitioning_opts = bigquery.external_config.HivePartitioningOptions()
    # The layout of the files in here is compatible with the layout requirements for hive partitioning,
    # so we can add an optional Hive partitioning configuration to leverage the object paths for deriving
    # partitioning column information.
    # For more information on how partitions are extracted, see:
    # https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs
    # We have a "/dt=YYYY-MM-DD/" path component in our example files as documented above.
    # Autolayout will expose this as a column named "dt" of type DATE.
    hive_partitioning_opts.mode = "AUTO"
    hive_partitioning_opts.require_partition_filter = False
    hive_partitioning_opts.source_uri_prefix = source_uri_prefix
    external_config.hive_partitioning = hive_partitioning_opts
    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(
            table.project, table.dataset_id, table.table_id)
    )
    # [END bigquery_create_table_external_hivepartitioned]
    return table


def check_table_exists(table_id, project):
    try:
        bigquery.Client(project=project).get_table(table_id)
        return True
    except NotFound:
        return False


# now you have to check if the rdd is empty, because you can't write a null DF on a bucket.
path_table = project_id + layer_dataset + table_id
path_bucket_table = bucket + layer_dataset + table_id

if tmp.rdd.isEmpty():
    print("NULL DF")
else:
    write_gcs(tmp, 'overwrite', vpath, 'DAT_TABLE')
    if check_table_exists(path_table, path_bucket_table) is False:
        create_table_external_hive_partitioned(
            path_table, path=vpath, project=path_bucket_table)
    else:
        print("Already exist table")
