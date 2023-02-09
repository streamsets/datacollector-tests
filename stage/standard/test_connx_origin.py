# Copyright 2023 StreamSets Inc.
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
import string

import pytest
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.connx, sdc_min_version('5.4.0')]


# https://www.connx.com/products/connx/CONNX%2013.5%20UserGuide/default.htm#connxcdd32c/ole_db_data_types.htm
# Omitting BIGINT and TIME for the time being since ConnX when used in the underlying MySQL table ConnX maps both of
# these types to VARCHAR instead, as according to the documentation ConnX decides the mapping of types to the SQL Type it
# deems most appropiate.
DATA_TYPES_CONNX = [
    ('TINYINT', '-128', 'SHORT', -128),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    #('BIGINT', '9223372036854775807', 'LONG', '9223372036854775807'),
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    ('REAL', '5.2', 'FLOAT', '5.2'),
    ('BIT',"'0'", 'BOOLEAN', False),
    ('DATE', "'2019-01-01'", 'DATE', 1546297200000),
    ('TIMESTAMP', "'2019-01-01 5:00:00'", 'DATETIME', 1546315200000),
    #('TIME', "'5:00:00'", 'TIME', 18000000),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('LONGVARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('BINARY', "'10101010'", 'BYTE_ARRAY', 'qg=='),
    ('VARBINARY(8)', "'10101010'", 'BYTE_ARRAY', 'qg=='),
    ('LONGVARBINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
]


@pytest.mark.parametrize('connx_type,insert_fragment,expected_type,expected_value', DATA_TYPES_CONNX, ids=[i[0] for i in DATA_TYPES_CONNX])
def test_data_types(sdc_builder, sdc_executor, connx_type, connx, insert_fragment, expected_type, expected_value, keep_data):
    """Test all feasible ConnX types."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    try:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                # Create table
                cursor.execute(f"""
                    CREATE TABLE {table_name}(
                        id int primary key,
                        data_column {connx_type} NULL
                    )
                """)

                # And insert a row with actual value
                cursor.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
                # And a null
                cursor.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('ConnX')
        origin.sql_query = 'SELECT * FROM {0}'.format(table_name)
        origin.incremental_mode = False
        origin.on_unknown_type = 'CONVERT_TO_STRING'

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(connx)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s ...', table_name)
            with connx.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"drop table {table_name}")


def test_object_names(sdc_builder, sdc_executor, connx):
    pytest.skip("The ConnX origin doesn't generate queries - it only takes user input, thus user is responsible to"
                "properly escape or enclose names and thefore there is not much for us to test here.")


@pytest.mark.parametrize('incremental', [True, False])
def test_multiple_batches(sdc_builder, sdc_executor, connx, incremental, keep_data):
    max_batch_size = 20
    batches = 10
    table_name = get_random_string(string.ascii_lowercase, 20)

    with connx.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(f"""
                CREATE TABLE {table_name}(
                    id int primary key
                )
            """)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('ConnX')
    origin.incremental_mode = incremental
    origin.sql_query = 'SELECT * FROM {0} WHERE '.format(table_name) + 'id > ${OFFSET} ORDER BY id'
    origin.initial_offset = '0'
    origin.offset_column = 'id'
    origin.max_batch_size_in_records = max_batch_size

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    origin >= finisher

    pipeline = builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Inserting data into %s', table_name)
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                for n in range(1, max_batch_size * batches + 1):
                    cursor.execute(f"INSERT INTO {table_name} VALUES({n})")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sortFunc(entry):
            return entry.field['id'].value

        records.sort(key=sortFunc)

        expected_number = 1
        for record in records:
            assert record.field['id'] == expected_number
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s ...', table_name)
            with connx.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"drop table {table_name}")


@pytest.mark.parametrize('incremental', [True, False]) # We have special handling for the events in incremental mode
def test_dataflow_events(sdc_builder, sdc_executor, connx, incremental, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)

    with connx.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(f"""
                CREATE TABLE {table_name}(
                    id int primary key
                )
            """)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('ConnX')
    origin.incremental_mode = incremental
    origin.sql_query = 'SELECT * FROM {0} WHERE '.format(table_name) + 'id > ${OFFSET} ORDER BY id'
    origin.initial_offset = '0'
    origin.offset_column = 'id'

    trash = builder.add_stage("Trash")
    origin >> trash

    wiretap = builder.add_wiretap()
    origin >= wiretap.destination

    pipeline = builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Inserting data into %s', table_name)
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                for n in range(1, 101):
                    cursor.execute(f"INSERT INTO {table_name} VALUES({n})")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(103)
        sdc_executor.stop_pipeline(pipeline)

        # In incremental mode, the query runs two times. The first time, the only event generated will be a
        # successful query. When the query runs again with the last offset, returning an empty set, both another succesful
        # query and a no-more-data are generated.
        # In the non incremental mode, the query runs a single time and generates a succesful query and a no-more-data
        # in one go
        records = wiretap.output_records
        if incremental:
            assert len(records) == 3
        else:
            assert len(records) == 2

        # First event is always a successful query
        assert records[0].header.values['sdc.event.type'] == 'jdbc-query-success'
        assert records[0].field['offset'] == '100' if incremental else '0'
        assert records[0].field['rows'] == 100
        assert 'timestamp' in records[0].field
        assert 'query' in records[0].field

        if incremental:
            # Second event for incremental is another succesful query
            assert records[1].header.values['sdc.event.type'] == 'jdbc-query-success'
            assert records[1].field['offset'] == '100' if incremental else '0'
            assert records[1].field['rows'] == 0
            assert 'timestamp' in records[1].field
            assert 'query' in records[1].field

            # Third event for incremental is no-more-data
            assert records[2].header.values['sdc.event.type'] == 'no-more-data'
            assert records[2].field['record-count'] == 100
        else:
            # Second event for non incremental is no-more-data
            assert records[1].header.values['sdc.event.type'] == 'no-more-data'
            assert records[1].field['record-count'] == 100
    finally:
        if not keep_data:
            logger.info('Dropping table %s ...', table_name)
            with connx.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"drop table {table_name}")


def test_data_format(sdc_builder, sdc_executor, connx, keep_data):
    pytest.skip("ConnX Origin doesn't deal with data formats")


def test_resume_offset(sdc_builder, sdc_executor, connx, keep_data):
    iterations = 3
    records_per_iteration = 10
    table_name = get_random_string(string.ascii_lowercase, 20)

    with connx.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(f"""
                CREATE TABLE {table_name}(
                    id int primary key
                )
            """)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('ConnX')
    origin.incremental_mode = True
    origin.sql_query = 'SELECT * FROM {0} WHERE '.format(table_name) + 'id > ${OFFSET} ORDER BY id'
    origin.initial_offset = '0'
    origin.offset_column = 'id'

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into %s', table_name)
            with connx.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Insert rows
                    for n in range(iteration * records_per_iteration + 1,
                                   iteration * records_per_iteration + 1 + records_per_iteration):
                        cursor.execute(f"INSERT INTO {table_name} VALUES({n})")

            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['id'].value == expected_number

                expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s ...', table_name)
            with connx.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"drop table {table_name}")
