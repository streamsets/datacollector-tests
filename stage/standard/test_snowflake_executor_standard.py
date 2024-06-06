# Copyright 2022 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import logging
import re
import string

import pytest
import pytz
import sqlalchemy
from streamsets.testframework.markers import snowflake, sdc_min_version, sdc_enterprise_lib_min_version
from streamsets.testframework.utils import get_random_string

pytestmark = [snowflake, sdc_min_version("3.20.0"), sdc_enterprise_lib_min_version({"snowflake": "1.9.0"})]

logger = logging.getLogger(__name__)

# Default database
DB = "STF_DB"
# Default schema
SCHEMA = "STF_SCHEMA"

DATA_TYPES_SHORT = [{"field": "field0", "type": "STRING"}, {"field": "field1", "type": "INTEGER"}]

DATA_TYPES_SNOWFLAKE = [
    # Boolean
    ("true", "BOOLEAN", "BOOLEAN", True),
    ("true", "BOOLEAN", "VARCHAR(4)", "true"),
    # Byte
    ("65", "BYTE", "CHAR(2)", "65"),
    # Short
    (120, "SHORT", "NUMBER", 120),
    (120, "SHORT", "CHAR(5)", "120"),
    (120, "SHORT", "VARCHAR(5)", "120"),
    (120, "SHORT", "NCHAR(5)", "120"),
    (120, "SHORT", "NVARCHAR2(5)", "120"),
    (120, "SHORT", "INTEGER", 120),
    (120, "SHORT", "INT", 120),
    (120, "SHORT", "BIGINT", 120),
    # Integer
    (120, "INTEGER", "NUMBER", 120),
    (120, "INTEGER", "CHAR(5)", "120"),
    (120, "INTEGER", "VARCHAR(5)", "120"),
    (120, "INTEGER", "NCHAR(5)", "120"),
    (120, "INTEGER", "NVARCHAR2(5)", "120"),
    (120, "INTEGER", "BIGINT", 120),
    (120, "INTEGER", "INTEGER", 120),
    (120, "INTEGER", "INT", 120),
    # Long
    (120, "LONG", "NUMBER", 120),
    (120, "LONG", "CHAR(5)", "120"),
    (120, "LONG", "VARCHAR(5)", "120"),
    (120, "LONG", "NCHAR(5)", "120"),
    (120, "LONG", "NVARCHAR2(5)", "120"),
    (120, "LONG", "BIGINT", 120),
    (120, "LONG", "INTEGER", 120),
    (120, "LONG", "INT", 120),
    # Float
    (120.0, "FLOAT", "NUMBER", 120.0),
    (120.0, "FLOAT", "CHAR(5)", "120.0"),
    (120.0, "FLOAT", "VARCHAR(5)", "120.0"),
    (120.0, "FLOAT", "NCHAR(5)", "120.0"),
    (120.0, "FLOAT", "NVARCHAR2(5)", "120.0"),
    (120.0, "FLOAT", "BIGINT", 120),
    (120.0, "FLOAT", "INTEGER", 120),
    (120.0, "FLOAT", "INT", 120),
    # Double
    (120.0, "DOUBLE", "NUMBER", 120.0),
    (120.0, "DOUBLE", "CHAR(5)", "120.0"),
    (120.0, "DOUBLE", "VARCHAR(5)", "120.0"),
    (120.0, "DOUBLE", "NCHAR(5)", "120.0"),
    (120.0, "DOUBLE", "NVARCHAR2(5)", "120.0"),
    (120.0, "DOUBLE", "BIGINT", 120),
    (120.0, "DOUBLE", "INTEGER", 120),
    (120.0, "DOUBLE", "INT", 120),
    (120.0, "DOUBLE", "DOUBLE", 120.0),
    (120.0, "DOUBLE", "DOUBLE PRECISION", 120.0),
    (120.0, "DOUBLE", "REAL", 120.0),
    # Decimal
    (120.0, "DECIMAL", "NUMBER", 120.0),
    (120.0, "DECIMAL", "CHAR(6)", "120.00"),
    (120.0, "DECIMAL", "VARCHAR(6)", "120.00"),
    (120.0, "DECIMAL", "NCHAR(6)", "120.00"),
    (120.0, "DECIMAL", "NVARCHAR2(6)", "120.00"),
    (120.0, "DECIMAL", "BIGINT", 120),
    (120.0, "DECIMAL", "INTEGER", 120),
    (120.0, "DECIMAL", "INT", 120),
    (120.0, "DECIMAL", "DOUBLE", 120.0),
    (120.0, "DECIMAL", "DOUBLE PRECISION", 120.0),
    (120.0, "DECIMAL", "REAL", 120.0),
    # Date
    ("2020-01-01 10:00:00", "DATE", "CHAR(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATE", "VARCHAR(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATE", "VARCHAR2(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATE", "NCHAR(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATE", "NVARCHAR2(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATE", "DATE", datetime.date(2020, 1, 1)),
    # Time
    ("10:00:00", "TIME", "TIME", datetime.time(10, 0, 0)),
    ("10:00:00", "TIME", "CHAR(50)", "Mon Dec 01 10:00:00 GMT 1969"),
    ("10:00:00", "TIME", "VARCHAR(50)", "Mon Dec 01 10:00:00 GMT 1969"),
    ("10:00:00", "TIME", "VARCHAR2(50)", "Mon Dec 01 10:00:00 GMT 1969"),
    ("10:00:00", "TIME", "NCHAR(50)", "Mon Dec 01 10:00:00 GMT 1969"),
    ("10:00:00", "TIME", "NVARCHAR2(50)", "Mon Dec 01 10:00:00 GMT 1969"),
    # DateTime
    ("2020-01-01 10:00:00", "DATETIME", "TIMESTAMP_NTZ", datetime.datetime(2020, 1, 1, 10, 0)),
    ("2020-01-01 10:00:00", "DATETIME", "TIMESTAMP_LTZ", datetime.datetime(2020, 1, 1, 10, 0)), # The expected will be modified by the test due to a bug in datetime
    ("2020-01-01 10:00:00", "DATETIME", "CHAR(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATETIME", "VARCHAR(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATETIME", "VARCHAR2(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATETIME", "NCHAR(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    ("2020-01-01 10:00:00", "DATETIME", "NVARCHAR2(50)", "Wed Jan 01 10:00:00 GMT 2020"),
    # Zoned DateTime
    (
        "2020-01-01T10:00:00+00:00",
        "ZONED_DATETIME",
        "TIMESTAMP_TZ",
        datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
    ),
    ("2020-01-01T10:00:00+00:00", "ZONED_DATETIME", "CHAR(50)", "2020-01-01T10:00Z"),
    ("2020-01-01T10:00:00+00:00", "ZONED_DATETIME", "VARCHAR(50)", "2020-01-01T10:00Z"),
    ("2020-01-01T10:00:00+00:00", "ZONED_DATETIME", "VARCHAR2(50)", "2020-01-01T10:00Z"),
    ("2020-01-01T10:00:00+00:00", "ZONED_DATETIME", "NCHAR(50)", "2020-01-01T10:00Z"),
    ("2020-01-01T10:00:00+00:00", "ZONED_DATETIME", "NVARCHAR2(50)", "2020-01-01T10:00Z"),
    # String
    ("string", "STRING", "CHAR(15)", "string"),
    ("string", "STRING", "VARCHAR(15)", "string"),
    ("string", "STRING", "NCHAR(15)", "string"),
    ("string", "STRING", "NVARCHAR2(15)", "string"),
    # Byte array - bytearray.fromhex('737472696e67').decode() == 'string'
    ("string", "BYTE_ARRAY", "BINARY", b"string"),
    ("string", "BYTE_ARRAY", "CHAR(15)", "c3RyaW5n"),
    ("string", "BYTE_ARRAY", "VARCHAR(15)", "c3RyaW5n"),
    ("string", "BYTE_ARRAY", "VARCHAR2(15)", "c3RyaW5n"),
    ("string", "BYTE_ARRAY", "NCHAR(15)", "c3RyaW5n"),
    ("string", "BYTE_ARRAY", "NVARCHAR2(15)", "c3RyaW5n"),
]

DATABASE_OBJECTS_NAMES = [
    ("random", f"STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}"),
    ("table", "TABLE"),
    ("select", "SELECT"),
    ("from", "FROM"),
    ("table*", "TABLE*"),
    ("ta$ble", "TA$BLE"),
    ("max_size", get_random_string(string.ascii_uppercase, 10)),
    ("plus", get_random_string(string.ascii_uppercase, 10) + "+" + get_random_string(string.ascii_uppercase, 10)),
    ("underscore", get_random_string(string.ascii_uppercase, 10) + "_" + get_random_string(string.ascii_uppercase, 10)),
    ("comma", get_random_string(string.ascii_uppercase, 10) + "," + get_random_string(string.ascii_uppercase, 10)),
    ("short", "A"),
]

ROWS_IN_DATABASE = [
    {"id": 1, "name": "Roger Federer"},
    {"id": 2, "name": "Rafael Nadal"},
    {"id": 3, "name": "Alexander Zverev"},
]

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = "snowflake"


def test_data_formats():
    pytest.skip("Snowflake Executor does not use a data format library.")


@pytest.mark.parametrize(
    "input_value,converter_type,db_type,expected",
    DATA_TYPES_SNOWFLAKE,
    ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_SNOWFLAKE],
)
def test_data_types(sdc_builder, sdc_executor, snowflake, input_value, converter_type, db_type, expected):
    """Test data types for the Snowflake Executor Stage. Data is pulled from a Dev Raw Data Source, converted
    to the corresponding SDC data type with a Field Type Converter and uploaded to Snowflake via the executor.
    The uploaded data is then pulled and compared to the expected data.

    Some data types, like dates and times, are more complex to input in the executor than others
    because they require some level of manual processing between the type conversion and the executor.
    For this reason each time has one of two pipelines.

    For simple conversions:
        origin >> type_converter >> [wiretap.destination, stage_snowflake_executor]
    For the complex conversions:
        origin >> type_converter >> stage_field_replacer >> [wiretap.destination, stage_snowflake_executor]
    """

    # Table and stage names.
    table_name = f"STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}"
    stage_name = f"STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}"

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f"{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}"

    # Map of Snowflake types and the conversion required to feed them to the executor.
    field_replacer_fields = {
        "DATE": "${time:extractStringFromDate(record:value('/VALUE'), 'yyyy-MM-dd HH:mm:ss')}",
        "TIME": "${time:extractStringFromDate(record:value('/VALUE'), 'HH:mm:ss')}",
        "TIMESTAMP_NTZ": "${time:extractStringFromDate(record:value('/VALUE'), 'yyyy-MM-dd HH:mm:ss')}",
        "TIMESTAMP_LTZ": "${time:extractStringFromDate(record:value('/VALUE'), 'yyyy-MM-dd HH:mm:ss')}",
    }

    if db_type == 'TIMESTAMP_LTZ': # this is needed due to a strange behavior (bug?) in datetime
        timezone = pytz.timezone('America/Los_Angeles')
        expected = datetime.datetime(2020, 1, 1, 10, 0)
        expected = timezone.localize(expected)

    # Create stage
    snowflake.create_stage(stage_name, storage_path)

    # Bind Snowflake engine to SQLAlchemy.
    meta = sqlalchemy.MetaData()
    meta.bind = snowflake.engine

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    # Origin.
    origin = builder.add_stage("Dev Raw Data Source")
    origin.data_format = "JSON"
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"VALUE": input_value})

    # Configure the field type converter in different ways for TIME and for the rest of types.
    type_converter = builder.add_stage("Field Type Converter")
    type_converter.conversion_method = "BY_FIELD"
    if converter_type == "TIME":
        type_converter.field_type_converter_configs = [
            {
                "fields": ["/VALUE"],
                "targetType": converter_type,
                "dataLocale": "en,US",
                "dateFormat": "OTHER",
                "otherDateFormat": "HH:MM:SS",
                "zonedDateTimeFormat": "ISO_OFFSET_DATE_TIME",
                "scale": 2,
            }
        ]
    else:
        type_converter.field_type_converter_configs = [
            {
                "fields": ["/VALUE"],
                "targetType": converter_type,
                "dataLocale": "en,US",
                "dateFormat": "YYYY_MM_DD_HH_MM_SS",
                "zonedDateTimeFormat": "ISO_OFFSET_DATE_TIME",
                "scale": 2,
            }
        ]

    # Build Snowflake executor stage.
    stage_snowflake_executor = builder.add_stage("Snowflake", type="executor")

    # How the value will be inserted into the query depends on it's type. Normal values are inserted
    # raw, byte arrays and binaries are encoded, and date, time or character related types are enquoted.
    value_in_query = "${record:value('/VALUE')}"
    if converter_type == "BYTE_ARRAY":
        value_in_query = "${base64:encodeBytes(record:value('/VALUE'), false)}"
    if db_type == "BINARY":
        value_in_query = f"(TO_BINARY('{value_in_query}', 'BASE64'))"

    for data_type in ("CHAR", "DATE", "TIME"):
        if data_type in db_type:
            value_in_query = f"'{value_in_query}'"
            break

    # Generate the query that the executor will run.
    insert_query = f"""INSERT INTO "{DB}"."{SCHEMA}".{table_name}
                VALUES ({value_in_query})"""

    # Add the query so that it will be run whenever the executor receives a result. The query
    # will insert (id, value) from the result into the database.
    stage_snowflake_executor.set_attributes(sql_queries=[insert_query], stage_name=stage_name)

    # Build the wiretap.
    wiretap = builder.add_wiretap()

    # Check if the data type requires a Field Replacer and if so configure it and add
    # it to the pipeline.
    replacement = field_replacer_fields.get(db_type)
    if replacement is not None:
        # Time has to be explicitly extracted as a string from the Time object,
        # which also includes the date.
        stage_field_replacer = builder.add_stage("Field Replacer")
        stage_field_replacer.set_attributes(
            replacement_rules=[{"fields": "/VALUE", "setToNull": False, "replacement": replacement}]
        )
        # Create pipeline with the field converter.
        origin >> type_converter >> stage_field_replacer >> [wiretap.destination, stage_snowflake_executor]
    else:
        # Use the default pipeline if no replacements are necessary.
        origin >> type_converter >> [wiretap.destination, stage_snowflake_executor]

    # Build pipeline.
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        snowflake.engine.execute(f'CREATE TABLE {table_name} ("VALUE" {db_type} NULL)')
        # Run pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # Fetch results from remote DB.
        result = snowflake.engine.execute(f'SELECT VALUE FROM "{table_name}"')
        rows = list(result)
        # Assert there is a single row and it has the expetec value.
        assert len(rows) == 1
        assert rows[0][0] == expected
    finally:
        _cleanup(snowflake, table_name, stage_name)


def test_dataflow_event(sdc_builder, sdc_executor, snowflake):
    """The Snowflake Executor can generate events when running Snowflake queries. This test
    wiretaps those events and asserts they are correct. The generated events contain the INSERT
    queries the executor run against snowflake. The test checks their validity by asserting that
    every provided by the Dev Raw Data Source is present in an INSERT query.

    Pipeline structure:
        dev_raw_data_source >> snowflake_executor >= [event_wiretap.destination, trash]
    """
    # Table and stage names.
    table_name = f"STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}"
    stage_name = f"STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}"

    # Create a source record for the test.
    source_records = [{"id": 1, "value": "val1"}, {"id": 2, "value": "val2"}]
    # Raw used in the Dev Raw Data Source, separating records with newlines.
    raw_data = "\n".join(json.dumps(record) for record in source_records)

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f"{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_uppercase, 10)}"

    # Bind Snowflake engine to SQLAlchemy.
    meta = sqlalchemy.MetaData()
    meta.bind = snowflake.engine

    # Define the table that will be used for the test.
    table = sqlalchemy.Table(
        table_name,
        meta,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("value", sqlalchemy.String),
    )

    # Initialize pipeline builder.
    builder = sdc_builder.get_pipeline_builder()

    # Build Snowflake stage.
    snowflake.create_stage(stage_name, storage_path)

    # Build Snowflake executor stage.
    snowflake_executor = builder.add_stage("Snowflake", type="executor")

    # Add Query that will run whenever the executor receives a result. The query
    # will insert (id, value) from the result into the database.
    snowflake_executor.set_attributes(
        sql_queries=[
            f"""INSERT INTO "{DB}"."{SCHEMA}"."{table_name}"
                VALUES (${{record:value('/id')}}, '${{record:value('/value')}}')"""
        ],
        stage_name=stage_name,
    )

    trash = builder.add_stage("Trash")

    # Create data source stage to generate (id: int, value: str) fields.
    dev_raw_data_source = builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(data_format="JSON", stop_after_first_batch=True, raw_data=raw_data)

    # Connect stages: dev_raw_data_source >> snowflake_executor.
    event_wiretap = builder.add_wiretap()
    dev_raw_data_source >> snowflake_executor >= [event_wiretap.destination, trash]

    # Build push pipeline.
    pull_pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pull_pipeline)

    try:
        # Create table that will be used in the test.
        table.create()
        # Start pipeline.
        sdc_executor.start_pipeline(pipeline=pull_pipeline).wait_for_finished()

        # Capture output events
        events = event_wiretap.output_records

        # Assert the number of events is the expected one, and that every
        assert len(events) == len(source_records), f"Wrong number of events ({len(events)} vs {len(source_records)})"

        # Create a set with the values found in the event queries so it's quicker to lookup the
        # expected values later on.
        actual_values = set()
        for event in events:
            query = str(event.field["query"])
            # extract values from query
            values = re.search(r"\(\d+, '.+'\)", query).group(0)
            actual_values.add(values)

        # The expected values are the literal characters of VALUES clause of the query,
        # e.g. (42, 'The answer').
        expected_values = [f"({r['id']}, '{r['value']}')" for r in source_records]

        # Assert all expected values have been inserted.
        for values in expected_values:
            assert values in actual_values, f"missing expected event values: {values}"
    finally:
        try:
            table.drop()
        finally:
            snowflake.drop_entities(stage_name=stage_name)


def test_multiple_batches(sdc_builder, sdc_executor, snowflake):
    """Test the Snowflake Executor with multiple batches. Batches are generated by a Dev Data Generator and
    uploaded by the executor before being fetched from snowflake and it is asserted that all of them were
    successfully uploaded.

    Pipeline structure:
        dev_data_generator >> [wiretap.destination, snowflake_executor]
    """
    table_name = f"STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}"
    stage_name = f"STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}"

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f"{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}"
    snowflake.create_stage(stage_name, storage_path)

    # Build the first pipeline with created entities in Snowflake stage configurations.
    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Data Generator
    dev_data_generator = builder.add_stage("Dev Data Generator")
    dev_data_generator.set_attributes(batch_size=10, delay_between_batches=1, fields_to_generate=DATA_TYPES_SHORT)

    # Build Snow Flake
    snowflake_executor = builder.add_stage("Snowflake", type="executor")
    insert_query = f"""INSERT INTO "{DB}"."{SCHEMA}"."{table_name}"
                VALUES ('${{record:value('/field0')}}', ${{record:value('/field1')}})"""
    snowflake_executor.set_attributes(sql_queries=[insert_query], stage_name=stage_name)

    wiretap = builder.add_wiretap()
    # Connect the pipeline.
    dev_data_generator >> [wiretap.destination, snowflake_executor]

    # Build first pipeline
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        stmt = f'CREATE TABLE "{DB}"."{SCHEMA}"."{table_name}" (field0 string, field1 integer, PRIMARY KEY(field1))'
        snowflake.engine.execute(stmt)

        # Run pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(30, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        # Assert match between from snowflake and data from wiretap.
        stmt = f'SELECT * FROM "{DB}"."{SCHEMA}"."{table_name}"'
        result = snowflake.engine.execute(stmt)
        records = [record.field.values() for record in wiretap.output_records]
        records_from_database = result.fetchall()
        assert len(records) == len(records_from_database)
        for i in range(len(records)):
            assert len(records[i]) == len(records_from_database[i])
            assert all(element in records_from_database[i].values() for element in records[i])
            assert all(element in records[i] for element in records_from_database[i])

    finally:
        _cleanup(snowflake, table_name, stage_name)


def test_multithreading():
    pytest.skip("Snowflake Executor does not use multithreading")


@pytest.mark.parametrize(
    "database_name_category,table_name", DATABASE_OBJECTS_NAMES, ids=[i[0] for i in DATABASE_OBJECTS_NAMES]
)
def test_object_names(sdc_builder, sdc_executor, snowflake, database_name_category, table_name):
    """Test for Snowflake Executor stage. Data from a Dev Raw Data Source is uploaded to Snowflake
    with an Executor stage, involving a reserved word in the process. Data is then fetched from the
    remote DB and it's validity is asserted.

    Pipeline structure:
    dev_raw_data_source >> snowflake_destination
    """

    # Schema and stage names. A random schema is found to avoid conflicts between object name tests of
    # multiple Snowflake stages.
    schema_name = f"STF_SCHEMA_{get_random_string(string.ascii_uppercase, 10)}"
    stage_name = f"STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}"

    # Create a table and stage in Snowflake.
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f"{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}"

    # The random schema is created to stage the files, we will reuse it for the table as well
    snowflake.create_stage(stage_name, storage_path, schema_name)

    # Create the schema manually.
    stmt = f'CREATE TABLE "{DB}"."{schema_name}"."{table_name}" (name string, id integer, PRIMARY KEY (id))'
    snowflake.engine.execute(stmt)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")

    raw_data = "\n".join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format="JSON", raw_data=raw_data, stop_after_first_batch=True)

    snowflake_executor = pipeline_builder.add_stage("Snowflake", type="executor")
    # Add query to the Snowflake executor. The query will insert (name, id) in the table.
    snowflake_executor.set_attributes(
        sql_queries=[
            f"""INSERT INTO "{DB}"."{schema_name}"."{table_name}"
                VALUES ('${{record:value('/name')}}', ${{record:value('/id')}})"""
        ],
        stage_name=stage_name,
    )
    # Connect the pipeline.
    dev_raw_data_source >> snowflake_executor

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        # Again, we need to take into account our custom schema
        stmt = f'SELECT * FROM "{DB}"."{schema_name}"."{table_name}"'
        result = snowflake.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row["name"], row["id"]) for row in ROWS_IN_DATABASE]
    finally:
        _cleanup(snowflake, table_name, stage_name, schema_name=schema_name)


@pytest.mark.parametrize("execution_mode", ["pull", "push"])
def test_pull_push(sdc_builder, sdc_executor, snowflake, execution_mode):
    """Test for Snowflake executor stage. If first checks a pull origin (Dev Raw Data Source) by generating records
    with the origin and inserting them into Snowflake with the executor, then comparing the data in Snowflake
    with the data expected to be there. The same thing is done for the push origin (Dev Data Generator).

    The pull pipeline looks like:
    stage_dev_data_generator >> stage_snowflake_executor

    The push pipeline looks like:
    stage_dev_raw_data_source >> stage_snowflake_executor
    """
    if execution_mode == "pull":
        _test_pull(sdc_builder, sdc_executor, snowflake)
    elif execution_mode == "push":
        _test_push(sdc_builder, sdc_executor, snowflake)


def test_start():
    pytest.skip("Snowflake executor in not used in Start events.")


def test_stop():
    pytest.skip("Snowflake executor is not used in Stop events.")


def _cleanup(snowflake, table_name, stage_name, schema_name=None):
    """Cleanup function that drops a table, stage and, if specified, an schema."""

    logger.info("Deleting table with name = %s...", table_name)
    if schema_name is None:
        query = f'DROP TABLE "{DB}"."{SCHEMA}"."{table_name}";'
        try:
            snowflake.engine.execute(query)
        finally:
            snowflake.drop_entities(stage_name=stage_name)
    else:
        query = f'DROP TABLE "{DB}"."{schema_name}"."{table_name}";'
        try:
            snowflake.engine.execute(query)
        finally:
            snowflake.drop_entities(stage_name=stage_name, schema_name=schema_name)
    snowflake.engine.dispose()


def _test_pull(sdc_builder, sdc_executor, snowflake):
    """Push test for Snowflake Executor. Upload data pulled from a Dev Raw Data Source to Snowflake and
    compare it to the expected data.

    Pipeline structure:
    dev_raw_data_source >> snowflake_executor
    """

    # Table and stage names.
    table_name = f"STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}"
    stage_name = f"STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}"

    # Create a source record for the test.
    source_records = [{"id": 1, "value": "val1"}, {"id": 2, "value": "val2"}]

    # Raw used in the Dev Raw Data Source, separating records with newlines.
    raw_data = "\n".join(json.dumps(record) for record in source_records)

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f"{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_uppercase, 10)}"

    # Bind Snowflake engine to SQLAlchemy.
    meta = sqlalchemy.MetaData()
    meta.bind = snowflake.engine

    # Define the table that will be used for the test.
    table = sqlalchemy.Table(
        table_name,
        meta,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("value", sqlalchemy.String),
    )

    # Initialize pipeline builder.
    builder = sdc_builder.get_pipeline_builder()

    # Build Snowflake stage.
    snowflake.create_stage(stage_name, storage_path)

    # Build Snowflake executor stage.
    snowflake_executor = builder.add_stage("Snowflake", type="executor")

    # Add Query that will run whenever the executor receives a result. The query
    # will insert (id, value) from the result into the database.
    snowflake_executor.set_attributes(
        sql_queries=[
            f"""INSERT INTO "{DB}"."{SCHEMA}".{table_name}
                VALUES (${{record:value('/id')}}, '${{record:value('/value')}}')"""
        ],
        stage_name=stage_name,
    )

    # Create data source stage to generate (id: int, value: str) fields.
    dev_raw_data_source = builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(data_format="JSON", stop_after_first_batch=True, raw_data=raw_data)

    # Connect the pipeline.
    dev_raw_data_source >> snowflake_executor

    # Build push pipeline.
    pull_pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pull_pipeline)

    try:
        # Create table that will be used in the test.
        table.create()

        # Start pipeline.
        sdc_executor.start_pipeline(pipeline=pull_pipeline).wait_for_finished()

        # Data expected from Snowflake. Records sorted by the id value
        expected_data = [(record["id"], record["value"]) for record in source_records]
        expected_sorted_data = sorted(expected_data, key=lambda row: row[0])

        # Get data stored in Snowflake database. Result sorted by the id value
        result = table.select().execute()
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        # Assert that obtained data matches expected data.
        assert data_from_database == expected_sorted_data, "mismatched results in push test for snowflake executor"
    finally:
        # Clean-up: drop table and stage
        try:
            table.drop()
        finally:
            snowflake.drop_entities(stage_name=stage_name)


def _test_push(sdc_builder, sdc_executor, snowflake):
    """Push test for Snowflake Executor. Upload data pushed by a Dev Data Generator to Snowflake and
    compare it to the expected data.

    Pipeline structure:
    dev_data_generator >> snowflake_executor
    """

    # Table and stage names.
    table_name = f"STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}"
    stage_name = f"STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}"

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f"{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_uppercase, 10)}"

    # Bind Snowflake engine to SQLAlchemy so it works correctly.
    meta = sqlalchemy.MetaData()
    meta.bind = snowflake.engine

    # The on_exit context will run cleanup calls on context exit.
    # Define the table that will be used for the test.
    table = sqlalchemy.Table(
        table_name,
        meta,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("value", sqlalchemy.String),
    )

    # Initialize pipeline builder.
    builder = sdc_builder.get_pipeline_builder()

    # Build Snowflake stage.
    snowflake.create_stage(stage_name, storage_path)

    # Build Snowflake executor stage.
    snowflake_executor = builder.add_stage("Snowflake", type="executor")

    # Add Query that will run whenever the executor receives a result. The query
    # will insert (id, value) from the result into the database.
    snowflake_executor.set_attributes(
        sql_queries=[
            f"""INSERT INTO "{DB}"."{SCHEMA}".{table_name}
                VALUES (${{record:value('/id')}}, '${{record:value('/value')}}')"""
        ],
        stage_name=stage_name,
    )

    # Create data generator stage to generate (id: int, value: str) fields.
    dev_data_generator = builder.add_stage("Dev Data Generator")
    dev_data_generator.set_attributes(
        batch_size=2,
        delay_between_batches=1,
        fields_to_generate=[{"field": "id", "type": "INTEGER"}, {"field": "value", "type": "STRING"}],
    )
    # Create wiretap to snoop the output of the data generator, which will
    # later be used to verify the functioning of the Snowflake executor.
    wiretap = builder.add_wiretap()

    # Connect stages: dev_data_generator >> snowflake_executor
    #                                    \_ wiretap
    dev_data_generator >> [wiretap.destination, snowflake_executor]

    # Build push pipeline.
    push_pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(push_pipeline)

    try:
        # Create table that will be used in the test.
        table.create()
        # Start pipeline and stop it after either 6 records or 300s.
        sdc_executor.start_pipeline(pipeline=push_pipeline).wait_for_pipeline_output_records_count(6, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline=push_pipeline)

        # Data expected from Snowflake. Records sorted by the id value
        expected_data = [(record.field.get("id"), record.field.get("value")) for record in wiretap.output_records]
        expected_sorted_data = sorted(expected_data, key=lambda row: row[0])

        # Get data stored in Snowflake database. Result sorted by the id value
        result = table.select().execute()
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        # Assert that obtained data matches expected data.
        assert data_from_database == expected_sorted_data, "mismatched results in push test for snowflake executor"

    finally:
        # Clean-up: drop table and stage
        try:
            table.drop()
        finally:
            snowflake.drop_entities(stage_name=stage_name)
