# Databricks notebook source
import logging
import pathlib
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
import concurrent.futures

from delta.tables import DeltaTable
from pyspark.shell import spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import Row, StringType, StructType, MapType, LongType, DoubleType
from pyspark.sql.utils import AnalysisException

# logger = logging.getLogger('py4j')

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

pyspark_log = logging.getLogger('py4j')
pyspark_log.setLevel(logging.ERROR)

spark.conf.set("spark.databricks.delta.schema.autoMerge", "true")
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
spark.conf.set("spark.hadoop.fs.s3a.credentialsType", "AssumeRole")
spark.conf.set("spark.hadoop.fs.s3a.stsAssumeRole.arn",
               "arn:aws:iam::764140241385:role/databricks-trusted-archive-role")
spark.conf.set("spark.hadoop.fs.s3a.canned.acl", "BucketOwnerFullControl")
spark.conf.set("spark.hadoop.fs.s3a.acl.default", "BucketOwnerFullControl")
spark.conf.set("spark.sql.streaming.stopActiveRunOnRestart", "true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

bucket_path = getArgument(
    "Bucket Path", "s3://skyscanner-data-platform-trusted-data-pipeline")
config_file_path = getArgument(
    "Config File Path", "/table-view-definitions/bronze-database.json")
preview_tables_path = getArgument("Preview Tables Path", "/preview-3/delta")
bronze_tables_path = getArgument("Bronze Tables Path", "/bronze-3/delta")

checkpointsPath = "s3://skyscanner-data-platform-trusted-data-pipeline/trusted-data-bronze/checkpoints/kinesis-9"
badRecordsPath = "s3://skyscanner-data-platform-trusted-data-pipeline/trusted-data-bronze/bad_records_path/kinesis-9"

# sample data for inferring the schemas
schema_sqs = spark.read.json(
    "s3://skyscanner-data-platform-trusted-data-pipeline/kinesisconnectortest/sample_sqs.json").schema
schema_sqs_message = spark.read.json(
    "s3://skyscanner-data-platform-trusted-data-pipeline/kinesisconnectortest/inner_sqs_message.json").schema


def read_table_config(database="bronze"):
    return spark.read.json(bucket_path + config_file_path).filter(col("database") == database).collect()


def table_matches_topic(table_config, topic):
    for source in table_config.sources:
        pp = pathlib.PurePath(topic)
        if pp.match(source.name):
            return True
    return False


def flatten_df(nested_df):
    ##what python black magic is this
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    
    # this should probably do a recursive call to flatten further nested structs
    flat_df = nested_df.select(*flat_cols, *[c + ".*" for c in nested_cols])
    return flat_df


def compatible_schemas(source_df, target_df):
    """compares the schemas of two dataframes.

    Returns True if source schemas is compatible with target, False if not"""

    flat_source_df = flatten_df(source_df)
    flat_target_df = flatten_df(target_df)
    source_schema = set(flat_source_df.schema.fields)
    target_schema = set(flat_target_df.schema.fields)
    return source_schema.issubset(target_schema)


def compatible_schemas_v2(source_df, target_s3_path):
    """compares the schemas of two dataframes.

    Returns True if source schemas is compatible with target, False if not"""

    try:
        source_df.where("1!=1").write.format("delta").mode("append").save(target_s3_path)
        return True
    except:
        pass
    return False


def merge_schemas(source_df, target_s3_path, on_failure="error"):
    """merge the schemas of a source dataframe and a target Delta table.

    source_df -- the source dataframe from which new columns should be loaded
    target_s3_path -- the s3 location of the target Delta table. New columns will be added here
    on_failure -- Two options: "error" and "overwrite".
                  In the case "error", an Exception will be raised if schemas are incompatible.
                  In the case of "overwrite", the data in the target table will be replaced with that of the
                  source_df, and the schema overwritten.

    Returns boolean: True if schemas were merged, False if no action was necessary
    """
    if on_failure not in ("error", "overwrite"):
        raise Exception("on_failure must be one of 'error' and 'overwrite'")

    # target_table = DeltaTable.forPath(spark, target_s3_path)
    # target_df = target_table.toDF()
    if compatible_schemas_v2(source_df, target_s3_path):
        logging.info("Schemas compatible, returning with no action")
        return False
    logging.info("Schemas not compatible, will try to merge")
    try:
        # We want to merge the schemas but not add any rows. We can achieve this by appending 0 rows.
        logging.info("merging schemas")
        source_df.where("1!=1").write.format("delta").option("mergeSchema", "true").mode("append").save(target_s3_path)
        time.sleep(2)
        return merge_schemas(source_df, target_s3_path, on_failure)
    except Exception as e:
        if on_failure == "error":
            raise Exception("could not merge schemas: {}".format(e))
        logging.info("Schemas not compatible, will overwrite")
        source_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(target_s3_path)
        time.sleep(2)
        return merge_schemas(source_df, target_s3_path, on_failure)


def delta_table_exists(path):
    return DeltaTable.isDeltaTable(identifier=path, sparkSession=spark)


def extract_dt(filename):
    for part in pathlib.Path(filename).parts:
        if part.startswith("dt="):
            return part[3:]
    raise Exception("filename does not have dt partition: {}".format(filename))


def flatten_list(some_list):
    return [item for sublist in some_list for item in sublist]


def extract_topic(filename):
    return pathlib.Path(filename).parts[3].strip()


def is_grappler_parquet(filename):
    return pathlib.Path(filename).parts[2] == 'grappler-parquet'


def move_utid_to_root(df):
    try:
        df = df.select(col('header.traveller_identity.utid').alias('utid'), '*')
        return df
    except:
        pass

    try:
        df = df.select(col('event_header.traveller_identity.utid').alias('utid'), '*')
        return df
    except:
        pass

    # assert False, "Failed to extract utid from dataframe: {}".format(df)
    return df


def merge_files_into_table(database, table, files):
    assert database in ("preview", "bronze"), "unhandled database name {}".format(database)

    df = spark.read.option("mergeSchema", "true").parquet(*files)

    dt_udf = udf(extract_dt, StringType())
    df = df.withColumn("dt", dt_udf(input_file_name()))
    df = move_utid_to_root(df)
    try:
        deduped_df = df.withColumn("guid", col("header.guid")) \
            .filter(col("guid").isNotNull()) \
            .dropDuplicates(["guid"]).cache()
    except AnalysisException:
        logging.info("Dataframe does not have guid column")
        return False
    #     logging.info(deduped_df)
    if database == "preview":
        output_path = preview_tables_path
    elif database == "bronze":
        output_path = bronze_tables_path

    output_path += "/" + table
    final_path = bucket_path + output_path
    exists = delta_table_exists(final_path)
    # logging.info("Directory exists: {} based on params {} {}".format(exists, bucket_path, output_path))
    if not exists:
        # create the table
        logging.info("Output path will be:{}".format(final_path))
        deduped_df. \
            write. \
            format("delta"). \
            partitionBy("dt"). \
            option("path", final_path). \
            option("mergeSchema", "true"). \
            mode("overwrite"). \
            save(final_path)
    else:
        # merge into the table
        logging.info("Will merge into table with path: {}".format(final_path))

        if database == "preview":
            merge_schemas(deduped_df, final_path, on_failure="overwrite")
        elif database == "bronze":
            merge_schemas(deduped_df, final_path, on_failure="error")

        # logging.info("writing to target")
        target = DeltaTable.forPath(spark, final_path)
        target.alias("target"). \
            merge(deduped_df.alias("source"), "target.guid = source.guid"). \
            whenNotMatchedInsertAll(). \
            execute()
        # logging.info("writing to target finished")


def process_batch(topic, files, item_number):
    if len(files) == 0:
        return

    logging.info("Processing batch of {} files for {}".format(len(files), topic))
    config = read_table_config()
    is_preview = True
    internal_table_name = topic
    for table in config:
        # logging.info("Checking config for table {}".format(table.name))
        if table_matches_topic(table, topic):
            # logging.info(" -- matches topic, this is a bronze table {}".format(topic))
            is_preview = False
            break
    if is_preview:
        # logging.info("No tables matched, this is a preview topic")
        merge_files_into_table(database="preview",
                               table=internal_table_name, files=files)
    else:
        # logging.info("Tables in bronze matched, this will be written to the internal table")
        merge_files_into_table(
            database="bronze", table=internal_table_name, files=files)
    
    return item_number


##################################################################################################
# Tests
##################################################################################################

def run_tests():
    test_table_matches_topic()
    test_is_grappler_parquet()
    test_delta_table_exists()
    test_extract_topic()
    test_extract_dt()
    test_compatible_schemas()


def test_table_matches_topic():
    sample_table_config = Row(database='bronze', name='frontend_mini_view',
                              sources=[Row(name='public.*.funnel_events.clients.View', type='topic')])
    test_cases = [
        {
            "table_config": sample_table_config,
            "topic_name": "public.service.funnel_events.clients.View",
            "matches": True,
            "description": "Check that glob pattern matches topic"
        },
        {
            "table_config": sample_table_config,
            "topic_name": "prod.service.funnel_events.clients.View",
            "matches": False,
            "description": "Check that non-matching topic is not matched"
        },
        {
            "table_config": sample_table_config,
            "topic_name": "prod.serv.ice.funnel_events.clients.View",
            "matches": False,
            "description": "Check that second dot forces non-match"
        },
    ]
    for tc in test_cases:
        got = table_matches_topic(tc["table_config"], tc["topic_name"])
        assert got == tc["matches"], "{}: table_matches_topic({}, {}) = {}, want {}".format(
            tc["description"],
            tc["table_config"],
            tc["topic_name"],
            got,
            tc["matches"],
        )

    logging.info("All tests passed!")


def test_delta_table_exists():
    test_cases = [
        ("bronze", False),
        ("fake-made-up-directory", False),
        ("s3://skyscanner-data-platform-trusted-data-pipeline/preview/delta/", False),
        ("s3://skyscanner-data-platform-trusted-data-pipeline/preview/delta/public_sandbox_ios_mini", False),
        (
            "s3://skyscanner-data-platform-trusted-data-pipeline/preview/delta/public_sandbox_ios_mini_clients_bookingpaneloption",
            True),
        ("s3://skyscanner-data-platform-trusted-data-pipeline/bronze/delta/prod.sol.funnel_events.clients.Search", True)
    ]
    for directory, want_exists in test_cases:
        got_exists = delta_table_exists(directory)
        assert got_exists == want_exists, "s3_directory_exists({}) = {}, want {}".format(directory, got_exists,
                                                                                         want_exists)


def test_extract_dt():
    test_cases = [
        (
            "s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public-sandbox.ios.mini.clients.BookingPanelOption/dt=2020-02-12/hour=11/region=eu-west-1/jobname=TrustedDataArchiver-2020-02-12T10-23-07/part-0-2821999",
            "2020-02-12"),
    ]
    for filepath, expected_dt in test_cases:
        got_dt = extract_dt(filepath)
        assert expected_dt == got_dt, "extracted date: {}, want {}".format(got_dt, expected_dt)


def test_extract_topic():
    test_cases = [
        (
            "s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public-sandbox.ios.mini.clients.BookingPanelOption/dt=2020-02-12/hour=11/region=eu-west-1/jobname=TrustedDataArchiver-2020-02-12T10-23-07/part-0-2821999",
            "public-sandbox.ios.mini.clients.BookingPanelOption"),
    ]
    for filepath, expected_dt in test_cases:
        got_topic = extract_topic(filepath)
        assert expected_dt == got_topic, "extracted date: {}, want {}".format(got_topic, expected_dt)


def test_is_grappler_parquet():
    test_cases = [
        (
            "s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public-sandbox.ios.mini.clients.BookingPanelOption/dt=2020-02-12/hour=11/region=eu-west-1/jobname=TrustedDataArchiver-2020-02-12T10-23-07/part-0-2821999",
            True),
        (
            "s3://skyscanner-data-platform-trusted-data-pipeline/audit-trail/public-sandbox.ios.mini.clients.BookingPanelOption/dt=2020-02-12/hour=11/region=eu-west-1/jobname=TrustedDataArchiver-2020-02-12T10-23-07/part-0-2821999",
            False)
    ]
    for filepath, expected_dt in test_cases:
        got = is_grappler_parquet(filepath)
        assert expected_dt == got, "is_grappler_parquet: {}, want {}".format(got, expected_dt)


def test_compatible_schemas():
    inner_struct = StructType() \
        .add("description", StringType()) \
        .add("ip", StringType()) \
        .add("id", LongType()) \
        .add("temp", LongType()) \
        .add("c02_level", LongType()) \
        .add("geo",
             StructType()
             .add("lat", DoubleType())
             .add("long", DoubleType()))

    inner_struct_addition = StructType() \
        .add("description", StringType()) \
        .add("ip", StringType()) \
        .add("id", LongType()) \
        .add("temp", LongType()) \
        .add("c02_level", LongType()) \
        .add("geo",
             StructType()
             .add("lat", DoubleType())
             .add("long", DoubleType())
             ).add("name", StringType())  # additional field, "name"

    schema1 = StructType() \
        .add("id", StringType()) \
        .add("header",
             MapType(
                 StringType(),
                 inner_struct))

    schema2 = StructType() \
        .add("id", StringType())  # no header

    schema3 = StructType() \
        .add("id", StringType()) \
        .add("header", inner_struct)  # incompatible type for header

    schema4 = StructType() \
        .add("id", StringType()) \
        .add("header", inner_struct_addition)  # additional field in inner struct

    df1 = spark.createDataFrame([], schema1)
    df2 = spark.createDataFrame([], schema2)
    df3 = spark.createDataFrame([], schema3)
    df4 = spark.createDataFrame([], schema4)

    assert compatible_schemas(df1, df1) is True
    assert compatible_schemas(df2, df2) is True
    assert compatible_schemas(df1, df2) is False
    assert compatible_schemas(df2, df1) is True

    assert compatible_schemas(df1, df3) is False
    assert compatible_schemas(df3, df1) is False

    assert compatible_schemas(df3, df4) is True
    assert compatible_schemas(df4, df3) is False


run_tests()


def for_each_batch(batch_df: DataFrame, batch_id):
    logging.info("Processing batch {}.".format(str(batch_id)))
    list_of_files = batch_df \
        .withColumn("decoded", from_json(col('data').cast("string"), schema=schema_sqs)) \
        .withColumn("message", from_json(col('decoded.Records.Sns.Message').cast("string"), schema=schema_sqs_message)) \
        .select(col("message.Records.s3.object.key")) \
        .rdd.flatMap(lambda x: x).collect()
    list_of_files = flatten_list(list_of_files)
    list_of_files = ["s3://skyscanner-data-platform-trusted-data-pipeline/" + item.replace("%3D", "=") for item in
                     list_of_files]
    list_of_files = list(filter(is_grappler_parquet, list_of_files))

    logging.info("Number of files: {}".format(len(list_of_files)))

    grouped = {}

    for f in list_of_files:
        key = extract_topic(f)
        if key in grouped.keys():
            grouped[key].append(f)
        else:
            grouped[key] = [f]

    grouped = [(k, v) for k, v in grouped.items()]

    # logging.info(grouped)
    #     grouped = [(k, list(g)) for k, g in groupby(list_of_files, lambda file: extract_topic(file))]

    logging.info("Got {} groups".format(len(grouped)))
    #     if len(grouped) > 0:
    #         item = grouped[0]
    
    with ThreadPoolExecutor(max_workers=64) as executor:
      futures =[]
      
      for k, item in enumerate(grouped):
        logging.info("Submitting item {} out of {}".format(k+1, len(grouped)))
        futures.append(executor.submit(process_batch,item[0], item[1], k))
        
      for f in concurrent.futures.as_completed(futures):
        try:
          item_number = f.result()
        except Exception as e:
          logging.error('{} generated the following exception {}'.format(f, e))
        else:
          logging.info('Future for item {} out of {} finished successfully'.format(item_number+1, len(grouped)))

#     for k, item in enumerate(grouped):
#         logging.info("Processing item {} out of {}".format(k+1, len(grouped)))
#         process_batch(item[0], item[1])


# smoke test
test = spark.read.parquet(
    "s3://skyscanner-data-platform-trusted-data-pipeline/kinesisconnectortest/part-00000-tid-6338391015268449108-db984cf1-b098-4d3e-9b1e-2cd96ef97684-30506-1-c000.snappy.parquet")
for_each_batch(test, 0)

stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", "trusted-data-pipeline-raw-to-bronze-kinesis") \
    .option("roleArn", "arn:aws:iam::764140241385:role/databricks-trusted-archive-role") \
    .option("roleSessionName", "rawtobronze-" + datetime.now().strftime("%d-%b-%Y_%H-%M-%S")) \
    .option("initialPosition", "latest") \
    .option("shardFetchInterval", "1m") \
    .load() \
    .writeStream \
    .option("checkpointLocation", checkpointsPath) \
    .option("badRecordsPath", badRecordsPath) \
    .foreachBatch(for_each_batch) \
    .trigger(processingTime='10 seconds') \
    .start()

# .option("maxFetchDuration", "5m") \
# .option("minFetchPeriod", "30s") \
# .option("shardFetchInterval", "1m")
# .option("maxFetchDuration", "2s") \
# .option("fetchBufferSize", "10mb") \
# .option("maxRecordsPerFetch", "1000") \

# COMMAND ----------

