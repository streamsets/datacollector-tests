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
import pytest
import sqlalchemy
import string

from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

from stage.utils.utils_migration import LegacyHandler as PipelineHandler
from stage.utils.utils_oracle import (
    DefaultConnectionParameters,
    DefaultStartParameters,
    DefaultTableParameters,
    cleanup,
    test_name,
    RECORD_FORMATS,
)


RELEASE_VERSION = "5.4.0"
ORACLE_CDC_ORIGIN = "Oracle CDC"
DEFAULT_TIMEOUT_IN_SEC = 120

# These variables need to be loaded only once and will not change from test to test
SERVICE_NAME = ""  # The value will be assigned during setup
SYSTEM_IDENTIFIER = ""  # The value will be assigned during setup
OK_STATUS = "0"

logger = logging.getLogger(__name__)
pytestmark = [database("oracle"), sdc_min_version(RELEASE_VERSION)]


@pytest.fixture()
def table_name():
    """Returns a random table name"""
    return get_random_string(string.ascii_uppercase, 10)


@pytest.mark.parametrize("record_format", RECORD_FORMATS)
@pytest.mark.parametrize(
    "oracle_type, oracle_value, streamsets_type, streamsets_value",
    [
        # ToDo Mikel not too convinced by the usage of sqlalchemy, maybe it's best to hardcode them
        (sqlalchemy.Integer, 1, "LONG", 1),
        # (sqlalchemy.Integer, None, "LONG", None),
        (sqlalchemy.CHAR(3), "ABC", "STRING", "ABC"),
        # (sqlalchemy.CHAR(3), None, "STRING", None),
        (sqlalchemy.VARCHAR(3), "ABC", "STRING", "ABC"),
        # ToDo Mikel VARCHAR2, DATETIME, other numerical types
        (sqlalchemy.NVARCHAR(3), "ABC", "STRING", "ABC"),
        # (sqlalchemy.NVARCHAR(3), None, "STRING", None),
        # (sqlalchemy.DATE, "TO_DATE('1998-1-1 6:22:33', 'YYYY-MM-DD HH24:MI:SS')", "DATETIME", 883635753000),
        # (sqlalchemy.DATE, None, 'DATETIME', None),
        # (sqlalchemy.TIMESTAMP, "TIMESTAMP'1998-1-2 6:00:00'", "DATETIME", 883720800000),
        # (sqlalchemy.TIMESTAMP, None, 'DATETIME', None),
        # (
        #     sqlalchemy.TIMESTAMP(timezone=True),
        #     "TIMESTAMP'1998-1-3 6:00:00-5:00'",
        #     "ZONED_DATETIME",
        #     "1998-01-03T06:00:00-05:00",
        # ),
        # (sqlalchemy.TIMESTAMP(timezone=True), "null", 'ZONED_DATETIME', None),
        # (sqlalchemy.BLOB, "utl_raw.cast_to_raw('BLOB')", "BYTE_ARRAY", "QkxPQg=="),
        # (sqlalchemy.BLOB, None, "BYTE_ARRAY", None),
        # (sqlalchemy.CLOB, "'CLOB'", "STRING", "CLOB"),
        # (sqlalchemy.CLOB, None, 'STRING', None),
    ],
)
def test_data_types(
    sdc_builder,
    sdc_executor,
    database,
    cleanup,
    table_name,
    test_name,
    record_format,
    oracle_type,
    oracle_value,
    streamsets_type,
    streamsets_value,
):
    """Test datatypes supported by the Oracle CDC Origin."""
    id_column = "ID"
    id_value = 1
    datatype_column = "VALUE"
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(id_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(datatype_column, oracle_type),
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
    connection.execute(table.insert({id_column: id_value, datatype_column: oracle_value}))
    txn.commit()

    handler.wait_for_metric(work, "output_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) == 1
    record = wiretap.output_records[0]

    # ToDo mikel: assert fail messages for all tests
    assert record.field[id_column].value == id_value
    assert record.field[datatype_column].value == streamsets_value
    assert record.field[datatype_column].type == streamsets_type


@pytest.mark.parametrize("record_format", RECORD_FORMATS)
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
    sdc_builder, sdc_executor, database, cleanup, table_name, test_name, record_format, object_name
):
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

    handler.wait_for_metric(pipeline, "output_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) == 1
    record = wiretap.output_records[0]
    assert record.field[id_column].value == id_value
    assert record.field[other_column].value == other_value


@pytest.mark.parametrize("record_format", RECORD_FORMATS)
@pytest.mark.parametrize("batches", [1, 10])
@pytest.mark.parametrize("max_batch_size", [1, 10, 1000])
def test_multiple_batches(
    sdc_builder, sdc_executor, database, cleanup, table_name, test_name, record_format, batches, max_batch_size
):
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

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'TRUNCATE'}"]

    oracle_cdc_origin >> wiretap.destination
    oracle_cdc_origin >= finisher

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    cleanup.callback(handler.stop_work, work)
    handler.start_work(work)

    txn = connection.begin()
    for value in expected_values:
        connection.execute(table.insert({id_column: value, other_column: value}))
    txn.commit()
    connection.execute(f"TRUNCATE TABLE {table_name}")

    handler.wait_for_status(pipeline, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) == len(expected_values)
    record_values = set(
        (record.field[id_column].value, record.field[other_column]) for record in wiretap.output_records
    )
    assert all([(value, value) in record_values] for value in expected_values)


def test_data_format(sdc_builder, sdc_executor, database):
    pytest.skip("Oracle CDC Origin doesn't deal with data formats.")


@pytest.mark.parametrize("iterations", [5])
def test_resume_offset(sdc_builder, sdc_executor, database, cleanup, table_name, test_name, record_format, iterations):
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
            pipeline, "output_record_count", records_per_iteration, timeout_sec=DEFAULT_TIMEOUT_IN_SEC
        )
        handler.stop_work(work)

    assert len(wiretap.output_records) == iterations * records_per_iteration
    record_values = set(
        (record.field[id_column].value, record.field[other_column]) for record in wiretap.output_records
    )
    assert all(
        [[(value, value) in record_values for value in iteration_values] for iteration_values in expected_values]
    )
