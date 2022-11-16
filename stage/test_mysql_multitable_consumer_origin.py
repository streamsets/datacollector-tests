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
import random
import string
import time

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.utils import get_random_string

from stage.utils.utils_primary_key_metadata import PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL, \
    PRIMARY_KEY_NUMERIC_METADATA_MYSQL, PRIMARY_KEY_MYSQL_TABLE, \
    get_create_table_query_non_numeric, get_create_table_query_numeric, get_insert_query_non_numeric, \
    get_insert_query_numeric, PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL_PRE_V8, \
    PRIMARY_KEY_NUMERIC_METADATA_MYSQL_PRE_V8, PRIMARY_KEY_MYSQL_PRE_V8_TABLE

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.sdc_min_version('5.4.0'), pytest.mark.database('mysql'), pytest.mark.skip]

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Ghastly'},
    {'id': 2, 'name': 'Haunter'},
    {'id': 3, 'name': 'Gengar'}
]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]


# SDC-11009: Run away pipeline runners in MySQL Multithread origins when no-more-data generation delay is configured
def test_mysql_multitable_consumer_with_no_more_data_event_generation_delay(sdc_builder, sdc_executor, database):
    """
    Make sure that when a delayed no-more-data is being processed, the pipeline properly waits on the processing to
    finish before stopping.

    source >> trash
           >= delay (only for no-more-data) >> trash
    """
    src_table = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    mysql_multitable_consumer = pipeline_builder.add_stage('MySQL Multitable Consumer')
    mysql_multitable_consumer.no_more_data_event_generation_delay_in_seconds = 1
    mysql_multitable_consumer.tables = [{"tablePattern": f'%{src_table}%'}]

    trash = pipeline_builder.add_stage('Trash')

    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 10 * 1000
    delay.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    trash_event = pipeline_builder.add_stage('Trash')

    mysql_multitable_consumer >> trash
    mysql_multitable_consumer >= delay
    delay >> trash_event

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        table = sqlalchemy.Table(
            src_table,
            metadata,
            sqlalchemy.Column('serial', sqlalchemy.Integer, primary_key=True)
        )
        table.create(database.engine)

        rows = [{'serial': 1}]
        connection.execute(table.insert(), rows)

        # We start the pipeline
        sdc_executor.start_pipeline(pipeline)

        # We wait three seconds - one second for the no-more-data to be generated and then some buffer time
        time.sleep(3)

        # Then we try to stop the pipeline, now the pipeline should not stop immediately and should in-fact wait
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()
        current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert current_status == 'STOPPED'

        # Validate expected metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        # Total number of input records
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        # 1 record, 1 no-more-data (rest of events is discarded)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 2
        # The table itself contained only one record
        assert history.latest.metrics.counter('stage.Trash_01.inputRecords.counter').count == 1
        # Only no-more-data event should reach the destination
        assert history.latest.metrics.counter('stage.Trash_02.inputRecords.counter').count == 1
        # The max batch time should be slightly more then 10 (the delayed batch that we have caused)
        # TODO: TLKT-167: Add access methods to metric objects
        assert history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('max') >= 10
    finally:
        if table is not None:
            table.drop(database.engine)


# SDC-10987: MySQL Multitable Consumer multiple offset columns with initial offset
def test_mysql_multitable_consumer_initial_offset_at_the_end(sdc_builder, sdc_executor, database):
    """
    Set initial offset at the end of the table and verify that no records were read.
    """
    table_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    mysql_multitable_consumer = builder.add_stage('MySQL Multitable Consumer')
    mysql_multitable_consumer.tables = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": ["id"],
        "offsetColumnToInitialOffsetValue": [{
            "key": "id",
            "value": "5"
        }]
    }]

    trash = builder.add_stage('Trash')

    mysql_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # Since the pipeline is not meant to read anything, we 'simply' wait
        time.sleep(5)

        sdc_executor.stop_pipeline(pipeline)

        # There must be no records read
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-11324: MySQL MultiTable origin can create duplicate offsets
def test_mysql_multitable_duplicate_offsets(sdc_builder, sdc_executor, database):
    """Validate that we will not create duplicate offsets. """
    table_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": table_name}]
    origin.max_batch_size_in_records = 1

    trash = pipeline_builder.add_stage('Trash')

    origin >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        # We should have transition 4 records
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == len(ROWS_IN_DATABASE)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == len(ROWS_IN_DATABASE)

        # And most importantly, validate offset
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert offset['offsets'] is not None
        expected_offset = {
            f"tableName={table_name};;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=false": "id=3",
            "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$": "2"
        }
        assert offset['offsets'] == expected_offset
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-11326: MySQL MultiTable origin forgets offset of non-incremental table on consecutive execution
def test_mysql_multitable_lost_nonincremental_offset(sdc_builder, sdc_executor, database):
    """Validate the origin does not loose non-incremental offset on various runs."""
    table_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": table_name, "enableNonIncremental": True}]
    origin.max_batch_size_in_records = 1

    trash = pipeline_builder.add_stage('Trash')

    origin >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        # We should have read all the records
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == len(ROWS_IN_DATABASE)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == len(ROWS_IN_DATABASE)

        # And most importantly, validate offset
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert offset['offsets'] is not None
        expected_offset = {
            f"tableName={table_name};;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=true": "completed=true",
            "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$": "2"
        }
        assert offset['offsets'] == expected_offset

        for _ in range(5):
            sdc_executor.start_pipeline(pipeline)

            # Since the pipeline won't read anything, give it few seconds to "idle"
            time.sleep(2)
            sdc_executor.stop_pipeline(pipeline)

            # And it really should not have read anything!
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0

            # And offset should not have changed
            offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
            assert offset is not None
            assert offset['offsets'] is not None
            assert offset['offsets'] == expected_offset

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


def test_mysql_multitable_consumer_origin_high_resolution_timestamp_offset(sdc_builder, sdc_executor, database):
    """
    Check if MySQL Multi-table Origin can retrieve any records from a table using as an offset a high resolution
    timestamp of milliseconds order. It is checked that the records read have a timestamp greater than the timestamp
    used as initial offset.

    Pipeline looks like:

    mysql_multitable_consumer >> wiretap
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    mysql_multitable_consumer = pipeline_builder.add_stage('MySQL Multitable Consumer')
    mysql_multitable_consumer.set_attributes(tables=[{'tablePattern': f'%{src_table_prefix}%',
                                                            'overrideDefaultOffsetColumns': True,
                                                            'offsetColumns': ['added'],
                                                            'offsetColumnToInitialOffsetValue': [{
                                                                'key': 'added',
                                                                'value': '${time:extractNanosecondsFromString(' +
                                                                         '"1996-12-02 00:00:00.020111000")}'
                                                            }]
                                                            }])

    wiretap = pipeline_builder.add_wiretap()
    mysql_multitable_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    connection = database.engine.connect()
    # Create table
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    connection.execute(f"""
                CREATE TABLE {table_name}(
                    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name varchar(100) NOT NULL,
                    age INT UNSIGNED NOT NULL,
                    added TIMESTAMP(6) NOT NULL
                )
            """)

    # Insert rows
    logger.info('Adding four rows into %s database ...', database.type)
    connection.execute(f'INSERT INTO {table_name} VALUES(1, "Charly", 14, "2005-02-08 14:00:00.100105002")')
    connection.execute(f'INSERT INTO {table_name} VALUES(2, "Paco", 28, "1992-05-25 11:00:00.000201010")')
    connection.execute(f'INSERT INTO {table_name} VALUES(3, "Eugenio", 21, "1996-12-01 23:00:00.020111")')
    connection.execute(f'INSERT INTO {table_name} VALUES(4, "Romualdo", 19, "2000-06-15 18:30:00.10523121")')

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3)
        sdc_executor.stop_pipeline(pipeline)

        name_id_from_output = [(record.field['name'], record.field['id'])
                               for record in wiretap.output_records]

        assert len(name_id_from_output) == 2
        assert name_id_from_output == [('Romualdo', 4), ('Charly', 1)]

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


# SDC-10053: MySQL Multitable does not handle large gap in primary keys
def test_mysql_multitable_consumer_partitioned_large_offset_gaps(sdc_builder, sdc_executor, database):
    """
    Ensure that the multi-table MySQL origin can handle large gaps between offset columns in partitioned mode
    The destination is wiretap, and there is a finisher waiting for the no-more-data event

    The pipeline will be started, and we will capture the information retrieved using a wiretap,
    then we assert those captured rows match the expected data.
    """

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    mysql_multitable_consumer = pipeline_builder.add_stage('MySQL Multitable Consumer')
    mysql_multitable_consumer.set_attributes(tables=[{
        "tablePattern": f'{table_name}',
        "enableNonIncremental": False,
        "partitioningMode": "REQUIRED",
        "partitionSize": "1000000",
        "maxNumActivePartitions": -1
    }])

    wiretap = pipeline_builder.add_wiretap()
    mysql_multitable_consumer >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']
    mysql_multitable_consumer >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding four rows into %s table, with a large gap in the primary keys ...', table_name)
        connection = database.engine.connect()
        rows_with_gap = ROWS_IN_DATABASE + [{'id': 5000000, 'name': 'Evil Jeff'}]
        connection.execute(table.insert(), rows_with_gap)
        connection.close()

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        rows_from_wiretap = [(record.get_field_data('/name').value, record.get_field_data('/id').value)
                              for record in wiretap.output_records]

        expected_data = [(row['name'], row['id']) for row in rows_with_gap]
        logger.info('Actual %s expected %s', rows_from_wiretap, expected_data)
        assert rows_from_wiretap == expected_data
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-13624:  MySQL Multitable Consumer ingests duplicates when initial offset is set for a column in partitioned mode
def test_mysql_multitable_consumer_duplicates_read_when_initial_offset_configured(sdc_builder, sdc_executor, database):
    """
    SDC-13625 Integration test for SDC-13624 - MT Consumer ingests duplicates when initial offset is specified
    Setup origin as follows:
        partitioning enabled + num_threads and num partitions > 1 + override offset column set
        + initial value specified for offset

    Verify that origin does not ingest the records more than once (duplicates) when initial value for offset is set

    Pipeline:
        MySQL MT Consumer >> Wiretap
                         >= Pipeline Finisher (no-more-data)
    """

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    mysql_multitable_consumer = pipeline_builder.add_stage('MySQL Multitable Consumer')
    mysql_multitable_consumer.set_attributes(tables=[{
        "tablePattern": f'{table_name}',
        "enableNonIncremental": False,
        "partitioningMode": "REQUIRED",
        "partitionSize": "100000",
        "maxNumActivePartitions": 5,
        'overrideDefaultOffsetColumns': True,
        'offsetColumns': ['created'],
        'offsetColumnToInitialOffsetValue': [{
            'key': 'created',
            'value': '0'
        }]
    }])

    mysql_multitable_consumer.number_of_threads = 2
    mysql_multitable_consumer.maximum_pool_size = 2

    wiretap = pipeline_builder.add_wiretap()
    mysql_multitable_consumer >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']
    mysql_multitable_consumer >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)
    ONE_MILLION = 1000000
    rows_in_table = [{'id': i, 'name': get_random_string(string.ascii_lowercase, 5), 'created': i + ONE_MILLION}
                     for i in range(1, 21)]

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(5)),
        sqlalchemy.Column('created', sqlalchemy.Integer)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding 20 rows into %s table', table_name)
        connection = database.engine.connect()

        connection.execute(table.insert(), rows_in_table)
        connection.close()

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows_from_wiretap = [(record.get_field_data('/name').value,
                               record.get_field_data('/id').value,
                               record.get_field_data('/created').value)
                              for record in wiretap.output_records]

        expected_data = [(row['name'], row['id'], row['created']) for row in rows_in_table]
        assert rows_from_wiretap == expected_data
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-14489: MySQL Multitable origin must escape string columns
@pytest.mark.parametrize('input_string', [
    "::problematical::value::",
    "=another=problematical=value=",
    ""
])
def test_multitable_string_offset_column(sdc_builder, sdc_executor, database, input_string):
    """Ensure that problematical values in String-typed offset column are covered, e.g. our special separator '::'."""

    builder = sdc_builder.get_pipeline_builder()
    table_name = get_random_string(string.ascii_letters, 10)

    origin = builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": f'{table_name}'}]
    origin.max_batch_size_in_records = 10

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.tables[0]["tablePattern"] = f'{table_name}'

    # Creating table with primary key that is String
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.String(60), primary_key=True, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id': input_string}])

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)

        # There should be no errors reported
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.MySQLMultitableConsumer_01.errorRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.MySQLMultitableConsumer_01.stageErrors.counter').count == 0

        # And verify that we properly read that one record
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].get_field_data('/id') == input_string
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@pytest.mark.parametrize('batch_strategy', ['SWITCH_TABLES', 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE'])
@pytest.mark.parametrize('no_of_threads', [1, 2, 3])
def test_mysql_multitable_consumer_batch_strategy(sdc_builder, sdc_executor, database, batch_strategy, no_of_threads):
    """
    Check if MySQL Multi-table Origin can load a couple of tables without duplicates using both batch_strategy options.
    Also it includes different thread combinations to ensure that with less, same and more threads works fine.

    mysql_multitable_consumer >> wiretap.destination
    mysql_multitable_consumer >= pipeline_finished_executor
    """
    no_of_records_per_table = random.randint(5001, 10000)

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name1 = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'
    table_name2 = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    mysql_multitable_consumer = pipeline_builder.add_stage('MySQL Multitable Consumer')
    mysql_multitable_consumer.set_attributes(tables=[{"tablePattern": f'{src_table_prefix}%'}],
                                            per_batch_strategy=batch_strategy,
                                            number_of_threads=no_of_threads,
                                            maximum_pool_size=no_of_threads)

    pipeline_finished_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = pipeline_builder.add_wiretap()

    mysql_multitable_consumer >> wiretap.destination
    mysql_multitable_consumer >= pipeline_finished_executor

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    def get_table_schema(table_name):
        return sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('field1', sqlalchemy.String(32)),
            sqlalchemy.Column('field2', sqlalchemy.String(32)),
            sqlalchemy.Column('field3', sqlalchemy.String(32)),
            sqlalchemy.Column('field4', sqlalchemy.String(32)),
            sqlalchemy.Column('field5', sqlalchemy.String(32)),
            sqlalchemy.Column('field6', sqlalchemy.String(32)),
            sqlalchemy.Column('field7', sqlalchemy.String(32)),
            sqlalchemy.Column('field8', sqlalchemy.String(32)),
            sqlalchemy.Column('field9', sqlalchemy.String(32))
        )

    def get_table_data(table):
        return [{'id': i, 'field1': f'field1_{i}_{table}', 'field2': f'field2_{i}_{table}',
                 'field3': f'field3_{i}_{table}', 'field4': f'field4_{i}_{table}',
                 'field5': f'field5_{i}_{table}', 'field6': f'field6_{i}_{table}',
                 'field7': f'field7_{i}_{table}', 'field8': f'field8_{i}_{table}',
                 'field9': f'field9_{i}_{table}'} for i in range(1, no_of_records_per_table + 1)]

    table1 = get_table_schema(table_name1)
    table2 = get_table_schema(table_name2)

    table_data1 = get_table_data(1)
    table_data2 = get_table_data(2)
    try:
        logger.info('Creating table %s in %s database ...', table_name1, database.type)
        logger.info('Creating table %s in %s database ...', table_name2, database.type)
        table1.create(database.engine)
        table2.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table1.insert(), table_data1)
        connection.execute(table2.insert(), table_data2)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = [{'id': record.field['id'], 'field1': record.field['field1'], 'field2': record.field['field2'],
                    'field3': record.field['field3'], 'field4': record.field['field4'],
                    'field5': record.field['field5'], 'field6': record.field['field6'],
                    'field7': record.field['field7'], 'field8': record.field['field8'],
                    'field9': record.field['field9']} for record in wiretap.output_records]

        assert len(records) == len(table_data1) + len(table_data2) == no_of_records_per_table * 2
        assert all(element in records for element in table_data1)
        assert all(element in records for element in table_data2)
    finally:
        logger.info('Dropping table %s in %s database...', table_name1, database.type)
        logger.info('Dropping table %s in %s database...', table_name2, database.type)
        table1.drop(database.engine)
        table2.drop(database.engine)


@pytest.mark.parametrize('test_data', [
    {'per_batch_strategy': 'SWITCH_TABLES', 'thread_count': 2, 'partition_size': 1000},
    {'per_batch_strategy': 'SWITCH_TABLES', 'thread_count': 3, 'partition_size': 1000},
    {'per_batch_strategy': 'SWITCH_TABLES', 'thread_count': 4, 'partition_size': 1000},
    {'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'thread_count': 2, 'partition_size': 1000},
    {'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'thread_count': 3, 'partition_size': 1000},
    {'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'thread_count': 4, 'partition_size': 1000},
    {'per_batch_strategy': 'SWITCH_TABLES', 'thread_count': 2, 'partition_size': 0},
    {'per_batch_strategy': 'SWITCH_TABLES', 'thread_count': 3, 'partition_size': 0},
    {'per_batch_strategy': 'SWITCH_TABLES', 'thread_count': 4, 'partition_size': 0},
    {'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'thread_count': 2, 'partition_size': 0},
    {'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'thread_count': 3, 'partition_size': 0},
    {'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'thread_count': 4, 'partition_size': 0}
])
def test_no_data_losses_or_duplicates_in_multithreaded_mode(sdc_builder, sdc_executor, database, test_data):
    """
    We want to make sure that there are no duplicates or data loses when the number of threads is less than
    the number of tables; is equal to the number of tables or is greater than the number of tables.
    And we want to run the same test for all batch strategies with partitioning enabled and disabled too.

    To run the test we first create 3 tables and populate them with 12000, 36000 and 6000 random records respectively.
    To prove the connector works as expected we will
    1) test that the number of records written is as expected for every table, not more (to exclude data duplicates)
    and not less (to exclude data loses).
    2) test that at the end we get all expected events: 3 table-finished events (1 for each table), 1 schema-finished
    event with 3 table names and 1 no-more-data event.

    The pipeline is as follows:

    MySQL Multitable Consumer >> Splitter >> Wiretap1 (12000 records)
                                Splitter >> Wiretap2 (36000 records)
                                Splitter >> Wiretap3 (6000 records)
                                Splitter >> Default Wiretap (0 records)
                             >= Event Wiretap

    """

    rows_in_tables = [12000, 36000, 6000]
    table_prefix = f'{get_random_string(string.ascii_lowercase, 6)}_'
    table_names = [f'{table_prefix}{get_random_string(string.ascii_lowercase, 20)}'
                   for _ in range(0, len(rows_in_tables))]
    tables = []
    all_rows = []
    pipeline = None

    try:
        all_row_count = 0
        for i, rows_in_table in enumerate(rows_in_tables):
            columns = [
                sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
                sqlalchemy.Column('NAME', sqlalchemy.String(32), quote=True)
            ]
            rows = [{'id': j + 1, 'NAME': get_random_string(string.ascii_lowercase, 32)} for j in range(rows_in_table)]
            all_rows += [rows]
            all_row_count += len(rows)
            table = sqlalchemy.Table(table_names[i], sqlalchemy.MetaData(), *columns, quote=True)
            table.create(database.engine)
            tables += [table]
            connection = database.engine.connect()
            connection.execute(table.insert(), rows)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        attributes = {
            'tables': [{
                "tablePattern": f'{table_prefix}%',
                'partitioningMode': 'REQUIRED' if test_data['partition_size'] > 0 else 'DISABLED',
                'partitionSize': str(test_data['partition_size'])
            }],
            'max_batch_size_in_records': 100,
            'fetch_size': 10,
            'number_of_threads': test_data['thread_count'],
            'maximum_pool_size': test_data['thread_count'],
            'per_batch_strategy': test_data['per_batch_strategy']
        }

        mysql_multitable_consumer = pipeline_builder.add_stage('MySQL Multitable Consumer')
        mysql_multitable_consumer.set_attributes(**attributes)

        stream_selector = pipeline_builder.add_stage('Stream Selector')

        mysql_multitable_consumer >> stream_selector

        event_wiretap = pipeline_builder.add_wiretap()
        mysql_multitable_consumer >= event_wiretap.destination

        wiretaps = []
        for _ in range(0, len(table_names) + 1):
            wiretap = pipeline_builder.add_wiretap()
            stream_selector >> wiretap.destination
            wiretaps += [wiretap]

        conditions = [{
            'outputLane': stream_selector.output_lanes[i],
            'predicate': f"${{record:attribute('jdbc.tables')=='{table_name}'}}"
        } for i, table_name in enumerate(table_names)]
        conditions += [{'outputLane': stream_selector.output_lanes[len(table_names)], 'predicate': 'default'}]

        stream_selector.condition = conditions

        pipeline = pipeline_builder.build().configure_for_environment(database)
        mysql_multitable_consumer.tables[0]["tablePattern"] = f'{table_prefix}%'

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', all_row_count, timeout_sec=300)

        for i, rows in enumerate(all_rows):
            assert len(rows) == len(wiretaps[i].output_records)
            output_records = [{
                'id': r.field['id'].value,
                'NAME': r.field['NAME'].value
            } for r in wiretaps[i].output_records]
            output_records.sort(key=lambda r: r['id'])
            assert rows == output_records
        assert 0 == len(wiretaps[len(wiretaps) - 1].output_records)

        finished_tables = set()
        schema_finished_tables = set()
        database_finished = False
        for record in event_wiretap.output_records:
            if record.header['values']['sdc.event.type'] == 'table-finished':
                assert record.field['table'].value.lower() in table_names
                finished_tables.add(record.field['table'])
            elif record.header['values']['sdc.event.type'] == 'schema-finished':
                assert len(schema_finished_tables) == 0
                assert len(record.field['tables']) == len(table_names)
                for table in record.field['tables']:
                    assert table.value.lower() in table_names
                    schema_finished_tables.add(table)

            elif record.header['values']['sdc.event.type'] == 'no-more-data':
                assert not database_finished
                database_finished = True
            else:
                pytest.fail(f"Unexpected event type: {record.header['values']['sdc.event.type']}")

        assert len(finished_tables) == len(table_names)
        assert len(schema_finished_tables) == len(table_names)
        assert database_finished

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline)
        for table in tables:
            table.drop(database.engine)


def test_mysql_primary_keys_headers(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"

    INPUT_DATA = [
        {'id': 1, 'name': 'The Lich King', 'game': 'World of Warcraft'},
        {'id': 2, 'name': 'Bowser', 'game': 'Super Mario Bros'},
        {'id': 3, 'name': 'Handsome Jack', 'game': 'Borderlands 2'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": table_name}]

    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), primary_key=True),
        sqlalchemy.Column('game', sqlalchemy.String(64))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), INPUT_DATA)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              len(INPUT_DATA),
                                              timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # We should have 3 records
        assert len(wiretap.output_records) == len(INPUT_DATA)

        # Check the primary keys metadata
        for record in wiretap.output_records:
            assert primary_key_specification in record.header.values
            assert record.header.values[primary_key_specification] is not None

            if Version(database.version) < Version('8.0.0'):
                primary_key_specification_expected = PRIMARY_KEY_MYSQL_PRE_V8_TABLE
            else:
                primary_key_specification_expected = PRIMARY_KEY_MYSQL_TABLE

            primary_key_specification_json = json.dumps(
                json.loads(record.header.values[primary_key_specification]),
                sort_keys=True
            )

            primary_key_specification_expected_json = json.dumps(
                json.loads(primary_key_specification_expected),
                sort_keys=True
            )

            assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


def test_mysql_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"

    pipeline_builder = sdc_builder.get_pipeline_builder()

    offsetCol = "my_int"

    origin = pipeline_builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": table_name, "enableNonIncremental": True,
                             "overrideDefaultOffsetColumns": True,
                             "offsetColumns": [offsetCol]} ]

    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        connection = database.engine.connect()
        connection.execute(get_create_table_query_numeric(table_name, database))

        logger.info('Adding a record into the table')
        connection.execute(get_insert_query_numeric(table_name, database))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              1,
                                              timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # We should have at least one record
        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert primary_key_specification in record.header.values
        assert record.header.values[primary_key_specification] is not None

        primary_key_specification_json = json.dumps(
            json.loads(record.header.values[primary_key_specification]),
            sort_keys=True
        )

        if Version(database.version) < Version('8.0.0'):
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_MYSQL_PRE_V8
        else:
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_MYSQL

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table {table_name}')


def test_mysql_non_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"

    pipeline_builder = sdc_builder.get_pipeline_builder()

    offsetCol = "my_date"

    origin = pipeline_builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": table_name, "enableNonIncremental": True,
                             "overrideDefaultOffsetColumns": True,
                             "offsetColumns": [offsetCol]}]

    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        connection = database.engine.connect()
        connection.execute(get_create_table_query_non_numeric(table_name, database))

        logger.info('Adding a record into the table')
        connection.execute(get_insert_query_non_numeric(table_name, database))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              1,
                                              timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # We should have at least one record
        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert primary_key_specification in record.header.values
        assert record.header.values[primary_key_specification] is not None

        primary_key_specification_json = json.dumps(
            json.loads(record.header.values[primary_key_specification]),
            sort_keys=True
        )

        if Version(database.version) < Version('8.0.0'):
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL_PRE_V8
        else:
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table {table_name}')


def test_mysql_vendor_header(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    vendor_specification = f"jdbc.vendor"

    INPUT_DATA = [
        {'id': 1, 'name': 'The Lich King', 'game': 'World of Warcraft'},
        {'id': 2, 'name': 'Bowser', 'game': 'Super Mario Bros'},
        {'id': 3, 'name': 'Handsome Jack', 'game': 'Borderlands 2'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('MySQL Multitable Consumer')
    origin.tables = [{"tablePattern": table_name}]

    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), primary_key=True),
        sqlalchemy.Column('game', sqlalchemy.String(64))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), INPUT_DATA)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              len(INPUT_DATA),
                                              timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # We should have 3 records
        assert len(wiretap.output_records) == len(INPUT_DATA)

        # Check the vendor metadata
        for record in wiretap.output_records:
            assert vendor_specification in record.header.values
            assert record.header.values[vendor_specification] is not None

            vendor_metadata = record.header.values[vendor_specification]

            vendor_metadata_expected = 'MySQL'

            assert vendor_metadata == vendor_metadata_expected

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


#
# Auxiliary methods
#


def _setup_delimited_file(sdc_executor, tmp_directory, csv_records):
    """Setup csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    raw_data = "\n".join(csv_records)
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='csv')

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # Generate some batches/files.
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=5)

    return csv_records


def _get_date_from_days(d):
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=d)

