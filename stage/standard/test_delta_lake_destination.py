# Copyright 2021 StreamSets Inc.

import json
import logging
from operator import itemgetter

import pytest
from streamsets.testframework.markers import aws, deltalake, sdc_min_version, sdc_enterprise_lib_min_version
from streamsets.testframework.utils import get_random_string

pytestmark = [deltalake, sdc_min_version('3.20.0')]

logger = logging.getLogger(__name__)

DESTINATION_STAGE_NAME = "com_streamsets_pipeline_stage_destination_DatabricksDeltaLakeDTarget"

@aws("s3")
@pytest.mark.parametrize(
    "data_value, converter_type, delta_lake_type, expected",
    [
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-datatypes.html
        # Numbers conversions
        (15, "BYTE", "BYTE", 15),
        (15, "BYTE", "SHORT", 15),
        (15, "BYTE", "INTEGER", 15),
        (15, "BYTE", "FLOAT", 15.0),
        (15, "SHORT", "SHORT", 15),
        (15, "SHORT", "INTEGER", 15),
        (15, "SHORT", "FLOAT", 15.0),
        (1000, "INTEGER", "INTEGER", 1000),
        (1000, "INTEGER", "SHORT", 1000),
        (100, "INTEGER", "BYTE", 100),
        (1000, "INTEGER", "LONG", 1000),
        (1000, "INTEGER", "FLOAT", 1000.0),
        (1000, "INTEGER", "DOUBLE", 1000.0),
        (100, "LONG", "BYTE", 100),
        (1000, "LONG", "SHORT", 1000),
        (1000, "LONG", "INTEGER", 1000),
        (100.12, "FLOAT", "FLOAT", 100.12),
        (100.12, "FLOAT", "DOUBLE", 100.12),
        (100.12, "FLOAT", "BYTE", 100),
        (100.12, "FLOAT", "SHORT", 100),
        (100.12, "FLOAT", "INTEGER", 100),
        # String
        ("Hello World", "STRING", "STRING", "Hello World"),
        # Boolean
        ("true", "BOOLEAN", "BOOLEAN", True),
        ("true", "BOOLEAN", "STRING", "true"),
        ("false", "BOOLEAN", "STRING", "false"),
        # Binary - bytearray.fromhex('737472696e67').decode() == 'string'
        ("string", "BYTE_ARRAY", "STRING", "737472696e67"),
        # Date
        # FIXME: The returned date is not correct. It seems that should be something messing up with timezones
        # In the FieldToStringFieldConverter DATE_CONVERTER the UTC timzeon is set. This could be the source of the problems
        # since DATE type for Deltalake is not considering any timezone
        # ('2020-01-01', 'DATE', 'DATE', datetime.datetime(2020, 1, 1)), # The returned date is not correct
        # ('20-01-01', 'DATE', 'STRING', '2020-01-01'), # The returned date is not correct
        # ("2020-01-01", 'DATE', 'TIMESTAMP', datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)), # The pipeline fails
        # Timestamp
        # ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'TIMESTAMP', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)), # The pipeline fails
        ("2020-01-01T10:00:00+00:00", "ZONED_DATETIME", "DATE", "2020-01-01"),
        (
                "2020-01-01T10:00:00+00:00",
                "ZONED_DATETIME",
                "STRING",
                "2020-01-01 10:00:00.000 +0000",
        ),
    ],
)
def test_data_types(
        sdc_builder,
        sdc_executor,
        deltalake,
        aws,
        data_value,
        converter_type,
        delta_lake_type,
        expected,
):
    """We need to test every datatype. Also as it is a destination, this tests
    will cover the most common conversion paths. e.g. INTEGER to FLOAT"""

    s3_key = f"stf-deltalake/{get_random_string()}"
    table_name = f"stf_test_data_types_{get_random_string()}"

    # Create the pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    converter = pipeline_builder.add_stage("Field Type Converter")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    dev_raw_data_source.set_attributes(
        data_format="JSON",
        raw_data=json.dumps({"value": data_value}),
        stop_after_first_batch=True,
    )
    converter.set_attributes(
        conversion_method="BY_FIELD",
        field_type_converter_configs=[
            {
                "fields": ["/value"],
                "targetType": converter_type,
                "dateFormat": "YYYY_MM_DD",
                "zonedDateTimeFormat": "ISO_OFFSET_DATE_TIME",
                "treatInputFieldAsDate": False,
                "dataLocale": "en,US",
                "encoding": "UTF-8",
                "scale": -1,
                "decimalScaleRoundingStrategy": "ROUND_UNNECESSARY",
            }
        ],
    )
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        purge_stage_file_after_ingesting=True,
        staging_file_format="CSV",
    )

    dev_raw_data_source >> converter >> databricks_deltalake
    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)


    # Now let's run the pipeline and assert the results
    try:
        connection = deltalake.connect_engine(deltalake.engine)
        connection.execute(
            f"create table {table_name} (value {delta_lake_type}) using delta location '/deltalake/{table_name}'"
        )

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        result = connection.execute(f"select * from {table_name}")
        rows = [row for row in result.fetchall()]
        result.close()

        assert len(rows) == 1
        assert rows[0][0] == expected

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)["KeyCount"] == 0
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")


# COMPLEX
# ArrayType(elementType, containsNull):
#   Represents values comprising a sequence of elements with the type of elementType
# MapType(keyType, valueType, valueContainsNull):
#   Represents values comprising a set of key-value pairs. The data type of keys is described by keyType and the data type of values is described by valueType
# StructType(fields):
#   Represents values with the structure described by a sequence of StructFields
# StructField(name, dataType, nullable):
#   Represents a field in a StructType.


@pytest.mark.skip("Currently not supported")
@pytest.mark.parametrize(
    "data_value, delta_lake_type, expected_value",
    [
        (
                {"VALUE": {"fullDocument": {"one": "two"}}},
                "MAP<STRING,MAP<STRING,STRING>>",
                {"fullDocument": {"one": "two"}},
        ),
        (
                {"VALUE": {"fullDocument": ["one", "two"]}},
                "MAP<STRING, ARRAY<STRING>>",
                {"fullDocument": ["one", "two"]},
        ),
        ({"VALUE": {"one": "two"}}, "MAP<STRING, STRING>", {"one": "two"}),
    ],
)
def test_data_types_map(
        sdc_builder,
        sdc_executor,
        deltalake,
        aws,
        data_value,
        delta_lake_type,
        expected_value,
):
    pass


@aws("s3")
@pytest.mark.parametrize(
    "table_name, fields, values, data_file_format",
    [
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-identifiers.html
        # Since the schema is handled different depending on the data file format, the tests are repeated for CSV and AVRO
        # Table names. Only letters, numbers and '_' are allowed
        (
                "test_table_name",
                [("test_column_1", "STRING")],
                {"test_column_1": "Hello World"},
                "CSV",
        ),
        (
                "test_table_1111",
                [("test_column_1", "STRING")],
                {"test_column_1": "Hello World"},
                "CSV",
        ),
        (
                "test_table_NAME",
                [("test_column_1", "STRING")],
                {"test_column_1": "Hello World"},
                "CSV",
        ),
        (
                "test_table_name",
                [("test_column_1", "STRING")],
                {"test_column_1": "Hello World"},
                "AVRO",
        ),
        (
                "test_table_1111",
                [("test_column_1", "STRING")],
                {"test_column_1": "Hello World"},
                "AVRO",
        ),
        (
                "test_table_NAME",
                [("test_column_1", "STRING")],
                {"test_column_1": "Hello World"},
                "AVRO",
        ),
        # For columns you can scape special characters with '`' except  ,;{}()\n\t=
        (
                "test_table_name",
                [("test_column_àccént", "STRING")],
                {"test_column_àccént": "Hello World"},
                "CSV",
        ),
        (
                "test_table_name",
                [("test_column_exclamation!", "STRING")],
                {"test_column_exclamation!": "Hello World"},
                "CSV",
        ),
        (
                "test_table_name",
                [("point.point", "STRING")],
                {"point.point": "Hello World"},
                "CSV",
        ),
        # ("test_table_name", [("`$%&!*+^:`", "STRING")], {"$%&!*+^:": "hola manola"}, "CSV"), # Error because of pyhive
        (
                "test_table_name",
                [("test_column_àccént", "STRING")],
                {"test_column_àccént": "Hello World"},
                "AVRO",
        ),
        # AVRO Does not support special characters for naming: https://avro.apache.org/docs/current/spec.html#names
        # ("test_table_name", [("olè!", "STRING")], {"olè!": "hola manola"}, "AVRO"),
        # ("test_table_name", [("punto.punto", "STRING")], {"punto.punto": "hola manola"}, "AVRO"),
        # ("test_table_name", [("`$%&!*+^:`", "STRING")], {"$%&!*+^:": "hola manola"}, "AVRO"), Error because of pyhive
    ],
)
def test_object_names(
        sdc_builder,
        sdc_executor,
        deltalake,
        aws,
        table_name,
        fields,
        values,
        data_file_format,
):
    """This test check out whether we properly handle problematic characters on
    object names. e.g. tables, files, topics,..."""

    # Append a random string to avoid race conditions on table creation/deletion
    # It does not interfere with the table name tests because the output of the
    # `get_random_string` function is always a valid table name
    table_name = f"{table_name}_{get_random_string()}"

    # Create the pipeline that will store data in the recently created table
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    dev_raw_data_source.set_attributes(
        data_format="JSON",
        raw_data=json.dumps(values),
        stop_after_first_batch=True,
    )
    s3_key = f"stf-deltalake/{get_random_string()}"
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        purge_stage_file_after_ingesting=True,
        staging_file_format=data_file_format,
    )

    dev_raw_data_source >> databricks_deltalake
    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        # Create table with special characters in table and fields
        connection = deltalake.connect_engine(deltalake.engine)
        columns = ",".join([f"`{c}` {t}" for c, t in fields])
        connection.execute(
            f"create table {table_name} ({columns}) using delta location '/deltalake/test_table_{get_random_string()}'"
        )

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        result = connection.execute(f"select * from {table_name}")
        rows = [row for row in result.fetchall()]

        assert len(rows) == 1
        assert rows[0][0] == list(values.values())[0]

        result.close()

        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)["KeyCount"] == 0
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")


@pytest.mark.skip("Deltalake destination does not generate events")
def test_dataflow_event(sdc_builder, sdc_executor, deltalake):
    """We test here each event the connector produces"""
    pass


@aws('s3')
def test_multiple_batches(sdc_builder, sdc_executor, deltalake, aws):
    """This test assert the connector handles properly multiple batches, including
    situations where every batch have different size. Additionally it tests all the
    different writing options"""
    batch_size = 5
    batches = 3
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage("Dev Data Generator")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    dev_data_generator.set_attributes(
        batch_size=batch_size,
        fields_to_generate=[{"type": "LONG_SEQUENCE", "field": "seq"}],
    )

    table_name = f"test_multiple_batches_{get_random_string()}"
    s3_key = f"stf-deltalake/{get_random_string()}"
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        purge_stage_file_after_ingesting=True,
    )

    dev_data_generator >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection = deltalake.connect_engine(deltalake.engine)
        connection.execute(
            f"create table {table_name} (seq INT) using delta location '/deltalake/test_multiple_batches_{get_random_string()}'"
        )

        sdc_executor.start_pipeline(pipeline, wait_for_statuses=["RUNNING"])
        sdc_executor.wait_for_pipeline_metric(pipeline, "data_batch_count", batches, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter("pipeline.batchInputRecords.counter").count
        result = connection.execute(f"select * from {table_name}")
        rows = [row for row in result.fetchall()]
        result.close()

        assert len(rows) == records

        rows.sort(key=lambda r: r[0])
        for i, row in enumerate(rows):
            assert row[0] == i

    finally:
        try:
            if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(pipeline)
        except Exception as ex:
            logger.error(f"Error encountered while stopping the pipeline. Exception: {ex}")
        try:
            aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        except Exception as ex:
            logger.error(f"Error encountered while deleting data in bucket: {aws.s3_bucket_name} key: {s3_key}")
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")

@aws('s3')
@pytest.mark.parametrize(
    "table_columns, data, data_configuration, expected",
    [
        (
                "text STRING, number INT",
                {"text": "Malaga", "number": 1},
                {
                    "staging_file_format": "CSV",
                    "row_field": "/",
                    "column_fields_to_ignore": "",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                ("Malaga", 1),
        ),
        (
                "number INT",
                {"text": "Malaga", "number": 1},
                {
                    "staging_file_format": "CSV",
                    "row_field": "/",
                    "column_fields_to_ignore": "text",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                (1,),
        ),
        (
                "text STRING",
                {"text": "Malaga", "number": 1},
                {
                    "staging_file_format": "CSV",
                    "row_field": "/",
                    "column_fields_to_ignore": "number",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                ("Malaga",),
        ),
        (
                "text STRING, number INT",
                {"text": "Random", "subfield": {"text": "Toledo", "number": 2}},
                {
                    "staging_file_format": "CSV",
                    "row_field": "/subfield",
                    "column_fields_to_ignore": "",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                ("Toledo", 2),
        ),
        (
                "number INT",
                {"text": "Random", "subfield": {"text": "Toledo", "number": 2}},
                {
                    "staging_file_format": "CSV",
                    "row_field": "/subfield",
                    "column_fields_to_ignore": "text",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                (2,),
        ),
        (
                "text STRING, number INT",
                {"text": "Malaga", "number": 1},
                {
                    "staging_file_format": "AVRO",
                    "row_field": "/",
                    "column_fields_to_ignore": "",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                ("Malaga", 1),
        ),
        (
                "number INT",
                {"text": "Malaga", "number": 1},
                {
                    "staging_file_format": "AVRO",
                    "row_field": "/",
                    "column_fields_to_ignore": "text",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                (1,),
        ),
        (
                "text STRING",
                {"text": "Malaga", "number": 1},
                {
                    "staging_file_format": "AVRO",
                    "row_field": "/",
                    "column_fields_to_ignore": "number",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                ("Malaga",),
        ),
        (
                "text STRING, number INT",
                {"text": "Random", "subfield": {"text": "Toledo", "number": 2}},
                {
                    "staging_file_format": "AVRO",
                    "row_field": "/subfield",
                    "column_fields_to_ignore": "",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                ("Toledo", 2),
        ),
        (
                "number INT",
                {"text": "Random", "subfield": {"text": "Toledo", "number": 2}},
                {
                    "staging_file_format": "AVRO",
                    "row_field": "/subfield",
                    "column_fields_to_ignore": "text",
                    "null_value": "\\N",
                    "merge_cdc_data": False,
                },
                (2,),
        ),
    ],
)
def test_data_format(
        sdc_builder,
        sdc_executor,
        deltalake,
        aws,
        table_columns,
        data,
        data_configuration,
        expected,
):
    # Create the pipeline that will store data in the recently created table
    table_name = f"test_data_format_{get_random_string()}"
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    dev_raw_data_source.set_attributes(
        data_format="JSON",
        raw_data=json.dumps(data),
        stop_after_first_batch=True,
    )
    s3_key = f"stf-deltalake/{get_random_string()}"
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        purge_stage_file_after_ingesting=True,
        **data_configuration,
    )

    dev_raw_data_source >> databricks_deltalake
    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)


    try:
        connection = deltalake.connect_engine(deltalake.engine)
        connection.execute(
            f"create table {table_name} ({table_columns}) using delta location '/deltalake/test_table_{get_random_string()}'"
        )

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        result = connection.execute(f"select * from {table_name}")
        rows = [row for row in result.fetchall()]
        result.close()

        assert len(rows) == 1
        assert rows[0] == expected
    finally:
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)["KeyCount"] == 0
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")


@pytest.mark.skip("To do")
def test_credential_store(sdc_builder, sdc_executor, deltalake, aws, credential_store):
    """Test configuring credentials with a real credentials store"""
    pass


@aws('s3')
@sdc_enterprise_lib_min_version({"databricks": "1.4.0"})
@pytest.mark.parametrize(
    "batch_size, batches, nthreads",
    [
        (10, 1, 2),
        (10, 1, 3),
        (10, 1, 4),
        (10, 3, 2),
        (10, 3, 3),
        (10, 3, 4),
    ],
)
def test_multithreading(sdc_builder, sdc_executor, deltalake, aws, batch_size, batches, nthreads):
    """Ensure the library works properly with multiple threads"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage("Dev Data Generator")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator.set_attributes(
        batch_size=batch_size,
        fields_to_generate=[{"type": "LONG_SEQUENCE", "field": "seq"}],
        number_of_threads=nthreads,
    )

    table_name = f"test_multithreading_{get_random_string()}"
    s3_key = f"stf-deltalake/{get_random_string()}"
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        purge_stage_file_after_ingesting=True,
    )

    dev_data_generator >> databricks_deltalake
    dev_data_generator >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection = deltalake.connect_engine(deltalake.engine)
        connection.execute(
            f"create table {table_name} (seq INT) using delta location '/deltalake/{table_name}_{get_random_string()}'"
        )

        # Wait for the required batches
        sdc_executor.start_pipeline(pipeline, wait_for_statuses=["RUNNING"])
        sdc_executor.wait_for_pipeline_metric(pipeline, "data_batch_count", batches)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter("pipeline.batchInputRecords.counter").count

        logger.info(f"Wrote {records} records")

        result = connection.execute(f"select * from {table_name}")
        rows = [row for row in result.fetchall()]
        result.close()

        wiretap_records = sorted([record.field["seq"].value for record in wiretap.output_records])
        deltalake_records = sorted([row[0] for row in rows])

        assert len(wiretap_records) == len(deltalake_records)
        assert wiretap_records == deltalake_records

    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")


@pytest.mark.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of test_multiple_batches "
                  "and Dev Raw Data Source (pull) is part of test_data_types.")
def test_push_pull(sdc_builder, sdc_executor, deltalake, aws):
    pass

@aws('s3')
@pytest.mark.parametrize('tags_size', [5, 20])
@sdc_min_version('5.7.0')
def test_s3_tags(sdc_builder, sdc_executor, deltalake, aws, tags_size):
    """Ensure the library works properly using s3 tags"""

    tags = {}
    for _ in range(tags_size):
        tags[get_random_string()] = get_random_string()
    s3_tags = []
    for key, value in tags.items():
        s3_tags.append({"key": key, "value": value})
    s3_tags = sorted(s3_tags, key=itemgetter('key'))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage("Dev Data Generator")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    dev_data_generator.set_attributes(
        records_to_be_generated=1,
        batch_size=1,
        fields_to_generate=[{"type": "LONG_SEQUENCE", "field": "seq"}]
    )

    table_name = f"test_s3_tags_{get_random_string()}"
    s3_key = f"stf-deltalake/{get_random_string()}"
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        s3_tags=s3_tags
    )

    dev_data_generator >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    databricks_deltalake.purge_stage_file_after_ingesting=False  # configure_for_environment() sets it to True!
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection = deltalake.connect_engine(deltalake.engine)
        connection.execute(
            f"create table {table_name} (seq INT) using delta location '/deltalake/{table_name}_{get_random_string()}'"
        )

        if len(tags) > 10:
            with pytest.raises(Exception) as error:
                sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
            assert "AWS_02" in error.value.message, f'Expected a AWS_02 error, got "{error.value.message}" instead'
        else:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

            s3_bucket_objects = aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)
            assert 'Contents' in s3_bucket_objects, f'Contents not found in response: {s3_bucket_objects}'
            keys = [k['Key'] for k in s3_bucket_objects['Contents']]
            assert any(keys), f'keys not found in Contents response: {s3_bucket_objects["Contents"]}'
            for key in keys:
                response = aws.s3.get_object_tagging(Bucket=aws.s3_bucket_name, Key=key)
                assert 'TagSet' in response, f'TagSet not found in response: {response}'
                response_tags = sorted(response['TagSet'], key=itemgetter('Key'))
                assert len(response_tags) == len(s3_tags), "number of tags differ!"
                diff = [(x, y) for x, y in zip(s3_tags, response_tags) if x['key'] != y['Key'] or x['value'] != y['Value']]
                assert not any(diff), f'tags do not match!'

    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")
