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


import logging
import os
import re
import pytest
import sqlalchemy
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects.oracle.base import ischema_names as oracle_names

from datetime import datetime, timedelta, timezone

from streamsets.testframework.markers import database, sdc_min_version

from stage.utils.common import cleanup
from stage.utils.utils_migration import LegacyHandler as PipelineHandler
from stage.utils.utils_oracle import (
    DefaultConnectionParameters,
    DefaultStartParameters,
    DefaultTableParameters,
    database_version,
    table_name,
    test_name,
    util_setup,
    RECORD_FORMATS,
)


RELEASE_VERSION = "5.4.0"
MIN_ORACLE_VERSION = 18
ORACLE_CDC_ORIGIN = "Oracle CDC"
DEFAULT_TIMEOUT_IN_SEC = 120

# These variables need to be loaded only once and will not change from test to test
SERVICE_NAME = ""  # The value will be assigned during setup
SYSTEM_IDENTIFIER = ""  # The value will be assigned during setup
OK_STATUS = "0"

DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_ZONED_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"
DEFAULT_DATETIME = datetime.strptime("1998-10-31 06:22:33", DEFAULT_DATETIME_FORMAT)
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo

logger = logging.getLogger(__name__)
pytestmark = [database("oracle"), sdc_min_version(RELEASE_VERSION)]


def format_timezone_offset(dt):
    """Add a colon separator between timezone hours and minutes.
    Convert YYYY-MM-DDTHH:MM:SS+ZZZZ to YYYY-MM-DDTHH:MM:SS+ZZ:ZZ"""

    dt_str = dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    pattern = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\+(\d{4})"
    match = re.compile(f"^{pattern}$").match(dt_str)
    if not match:
        return dt_str
    main_datetime = match.group(1)
    offset = match.group(2)
    hh, mm = offset[:2], offset[2:]
    postfix = "Z" if hh == mm == "00" else f"+{hh}:{mm}"
    return f"{main_datetime}{postfix}"


@pytest.mark.parametrize("null", [True, False])
@pytest.mark.parametrize(
    "type_name, oracle_type, oracle_value, streamsets_type, streamsets_value",
    [
        ["BINARY_DOUBLE", oracle.BINARY_DOUBLE, 4.2, "DOUBLE", 4.2],
        ["BINARY_FLOAT", oracle.BINARY_FLOAT, 4.2, "FLOAT", 4.2],
        ["CHAR", oracle.CHAR, "A", "STRING", "A"],
        ["DATE", oracle.DATE, DEFAULT_DATETIME, "DATETIME", DEFAULT_DATETIME],
        ["FLOAT", oracle.FLOAT, 4.2, "FLOAT", 4.2],
        ["NCHAR", oracle.NCHAR, "A", "STRING", "A"],
        ["NUMBER", oracle.NUMBER, 4, "DECIMAL", 4],
        ["NVARCHAR2", oracle.NVARCHAR2(length=3), "ABC", "STRING", "ABC"],
        ["TIMESTAMP", oracle.TIMESTAMP, DEFAULT_DATETIME, "DATETIME", DEFAULT_DATETIME],
        [
            "TIMESTAMP WITH LOCAL TIME ZONE",
            oracle.TIMESTAMP(timezone=True),
            DEFAULT_DATETIME.replace(tzinfo=LOCAL_TIMEZONE),
            "ZONED_DATETIME",
            format_timezone_offset(DEFAULT_DATETIME.replace(tzinfo=LOCAL_TIMEZONE)),
        ],
        [
            "TIMESTAMP WITH TIME ZONE",
            oracle.TIMESTAMP(timezone=True),
            DEFAULT_DATETIME.replace(tzinfo=LOCAL_TIMEZONE),
            "ZONED_DATETIME",
            format_timezone_offset(DEFAULT_DATETIME.replace(tzinfo=LOCAL_TIMEZONE)),
        ],
        ["VARCHAR", oracle.VARCHAR(length=3), "ABC", "STRING", "ABC"],
        ["VARCHAR2", oracle.VARCHAR2(length=3), "ABC", "STRING", "ABC"],
        # ["CHAR VARYING", oracle.CHAR VARYING],
        # ["CHARACTER", oracle.CHARACTER],
        # ["CHARACTER VARYING", oracle.CHARACTER VARYING],
        ["DECIMAL", oracle.NUMBER(asdecimal=True), 4, "DECIMAL", 4],
        #! ["DOUBLE PRECISION", oracle.DOUBLE_PRECISION, 4.2, "DOUBLE", 4.2],
        ["DOUBLE PRECISION", oracle.DOUBLE_PRECISION, 4.2, "FLOAT", 4.2],
        #! ["INT", sqlalchemy.INT, 42, "NUMBER", 42],
        ["INT", sqlalchemy.INT, 42, "LONG", 42],
        # ! ["INTEGER", sqlalchemy.INTEGER, 42, "NUMBER", 42],
        ["INTEGER", sqlalchemy.INTEGER, 42, "LONG", 42],
        # ["NATIONAL_CHAR", oracle.NATIONAL CHAR],
        # ["NATIONAL CHAR VARYING", oracle.NATIONAL CHAR VARYING],
        # ["NATIONAL CHARACTER", oracle.NATIONAL CHARACTER],
        # ["NATIONAL CHARACTER VARYING", oracle.NATIONAL CHARACTER VARYING],
        # ["NCHAR VARYING", oracle.NCHAR VARYING],
        # ["NUMERIC", oracle.NUMERIC],
        # ["REAL", oracle.REAL],
        # ["SMALLINT", oracle.SMALLINT],
    ],
)
def test_data_types(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    cleanup,
    table_name,
    test_name,
    null,
    type_name,
    oracle_type,
    oracle_value,
    streamsets_type,
    streamsets_value,
):
    """Test datatypes supported by the Oracle CDC Origin."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    id_column = "ID"
    id_value = 1
    datatype_column = "VALUE"
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(id_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(datatype_column, oracle_type),
    )
    # Test the null case for each data type
    if null:
        oracle_value = None
        streamsets_value = None

    connection = database.engine.connect()
    cleanup.callback(connection.close)

    table.create(database.engine)
    cleanup.callback(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    oracle_cdc_origin.set_attributes(
        missing_columns="FORWARD",
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database),
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    cleanup.callback(handler.stop_work, work)
    handler.start_work(work)

    txn = connection.begin()
    connection.execute(table.insert({id_column: id_value, datatype_column: oracle_value}))
    txn.commit()

    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) == 1
    record = wiretap.output_records[0]

    # ToDo mikel: assert fail messages for all tests
    assert record.field[id_column].value == id_value
    assert record.field[datatype_column].type == streamsets_type
    assert record.field[datatype_column].value == streamsets_value


@pytest.mark.parametrize(
    "object_name",
    [
        "TABLE",  # reserved words (must be quoted)
        "92TABLE",  # begin with numeric characters (must be quoted)
        "MY#$#TABLE_",  # allowed symbols (even for unquoted names)
        "myTABLE_upperANDlowerCaSEs",  # case sensitiveness (only when quoted)
        "TABLE_7D510B56T_219B_4DAB_AFAB",  # max length for a table name (30 bytes)
        "EVEN THIS & THAT!",  # allowed identifier when quoted
    ],
)
def test_object_named_tables(
    sdc_builder, sdc_executor, database, database_version, cleanup, table_name, test_name, object_name
):
    """Test the stage treats table names correctly even if they contain reserved keywords or
    characters that could otherwise break queries. """

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    id_column = "ID"
    id_value = 1
    other_column = "VALUE"
    other_value = 42
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(id_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(other_column, sqlalchemy.Integer),
    )

    connection = database.engine.connect()
    cleanup.callback(connection.close)

    table.create(database.engine)
    cleanup.callback(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    oracle_cdc_origin.set_attributes(
        null_columns="FORWARD",
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database),
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    cleanup.callback(handler.stop_work, work)
    handler.start_work(work)

    txn = connection.begin()
    connection.execute(table.insert({id_column: id_value, other_column: other_value}))
    txn.commit()

    handler.wait_for_metric(pipeline, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) == 1
    record = wiretap.output_records[0]
    assert record.field[id_column].value == id_value
    assert record.field[other_column].value == other_value


@pytest.mark.parametrize("batches", [1, 10])
@pytest.mark.parametrize("max_batch_size", [1, 10, 1000])
def test_multiple_batches(
    sdc_builder, sdc_executor, database, database_version, cleanup, table_name, test_name, batches, max_batch_size
):
    """Test the stage produces the expected number of batches with the expected size."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    id_column = "ID"
    other_column = "VALUE"
    expected_values = [i for i in range(batches * max_batch_size)]
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(id_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(other_column, sqlalchemy.Integer),
    )

    connection = database.engine.connect()
    cleanup.callback(connection.close)

    table.create(database.engine)
    cleanup.callback(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    oracle_cdc_origin.set_attributes(
        max_batch_size_in_records=max_batch_size,
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database),
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    cleanup.callback(handler.stop_work, work)
    handler.start_work(work)

    txn = connection.begin()
    for value in expected_values:
        connection.execute(table.insert({id_column: value, other_column: value}))
    txn.commit()

    handler.wait_for_metric(pipeline, "input_record_count", len(expected_values), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) == len(expected_values)
    record_values = set(
        (record.field[id_column].value, record.field[other_column]) for record in wiretap.output_records
    )
    assert all([(value, value) in record_values] for value in expected_values)


def test_data_format(sdc_builder, sdc_executor, database):
    pytest.skip("Oracle CDC Origin doesn't deal with data formats.")


@pytest.mark.parametrize("iterations", [5])
def test_resume_offset(
    sdc_builder, sdc_executor, database, database_version, cleanup, table_name, test_name, iterations
):
    """Test the stage resumes from the expected offset so it does"""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    records_per_iteration = 5
    id_column = "ID"
    other_column = "VALUE"
    expected_values = [
        [i for i in range(j, j + records_per_iteration)]
        for j in range(0, iterations * records_per_iteration, records_per_iteration)
    ]
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(id_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(other_column, sqlalchemy.Integer),
    )

    connection = database.engine.connect()
    cleanup.callback(connection.close)

    table.create(database.engine)
    cleanup.callback(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    oracle_cdc_origin.set_attributes(
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database)
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    cleanup.callback(handler.stop_work, work)

    for iteration in range(iterations):
        handler.start_work(work)

        txn = connection.begin()
        for value in expected_values[iteration]:
            connection.execute(table.insert({id_column: value, other_column: value}))
        txn.commit()

        handler.wait_for_metric(
            pipeline, "input_record_count", records_per_iteration, timeout_sec=DEFAULT_TIMEOUT_IN_SEC
        )
        handler.stop_work(work)

    assert len(wiretap.output_records) == iterations * records_per_iteration
    record_values = set(
        (record.field[id_column].value, record.field[other_column]) for record in wiretap.output_records
    )
    assert all(
        [[(value, value) in record_values for value in iteration_values] for iteration_values in expected_values]
    )


@pytest.mark.parametrize("threads", [1, 4, 8])
def test_multithreading(sdc_builder, sdc_executor, database, database_version, cleanup, test_name, table_name, threads):
    """Ensure the behaviour of the stage does not change with the number of parser threads."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    primary_column = "ID_COLUMN"
    secondary_column = "OTHER_COLUMN"
    base_time = datetime.strptime("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    record_count = 2000

    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(secondary_column, sqlalchemy.DATE),
    )
    connection = database.engine.connect()
    cleanup.callback(connection.close)

    table.create(database.engine)
    cleanup.callback(table.drop, database.engine)

    records = [{primary_column: i, secondary_column: base_time + timedelta(days=1) * i} for i in range(record_count)]

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    oracle_cdc_origin.set_attributes(
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database)
    )
    oracle_cdc_origin.set_attributes(sql_parser_threads=threads)

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    txn = connection.begin()
    connection.execute(table.insert(), records)
    txn.commit()

    cleanup.callback(handler.stop_work, work)
    handler.start_work(work)

    handler.wait_for_metric(work, "input_record_count", len(records), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    output_records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert output_records == records
