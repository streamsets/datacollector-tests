# Copyright 2020 StreamSets Inc.
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
import os
import random
import string
import tempfile
import time
from collections import OrderedDict

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

from stage.utils.utils_primary_key_metadata import PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL, \
    PRIMARY_KEY_NUMERIC_METADATA_MYSQL, \
    PRIMARY_KEY_NON_NUMERIC_METADATA_ORACLE, PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER, \
    PRIMARY_KEY_NON_NUMERIC_METADATA_POSTGRESQL, \
    PRIMARY_KEY_NUMERIC_METADATA_POSTGRESQL, PRIMARY_KEY_NUMERIC_METADATA_ORACLE, \
    PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER, \
    PRIMARY_KEY_ORACLE_TABLE, PRIMARY_KEY_SQLSERVER_TABLE, PRIMARY_KEY_POSTGRESQL_TABLE, PRIMARY_KEY_MYSQL_TABLE, \
    get_create_table_query_non_numeric, get_create_table_query_numeric, get_insert_query_non_numeric, \
    get_insert_query_numeric, PRIMARY_KEY_MARIADB_TABLE, PRIMARY_KEY_NUMERIC_METADATA_MARIADB, \
    PRIMARY_KEY_NON_NUMERIC_METADATA_MARIADB, PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL_PRE_V8, \
    PRIMARY_KEY_NUMERIC_METADATA_MYSQL_PRE_V8, PRIMARY_KEY_MYSQL_PRE_V8_TABLE

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie'},
    {'id': 4, 'name': 'Jarcec'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

DEFAULT_DB2_SCHEMA = 'DB2INST1'


# SDC-11009: Run away pipeline runners in JDBC Multithread origins when no-more-data generation delay is configured
@database
@sdc_min_version('3.2.0')
def test_jdbc_multitable_consumer_with_no_more_data_event_generation_delay(sdc_builder, sdc_executor, database):
    """
    Make sure that when a delayed no-more-data is being processed, the pipeline properly waits on the processing to
    finish before stopping.

    source >> trash
           >= delay (only for no-more-data) >> trash
    """
    src_table = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.no_more_data_event_generation_delay_in_seconds = 1
    jdbc_multitable_consumer.table_configs = [{"tablePattern": f'%{src_table}%'}]

    trash = pipeline_builder.add_stage('Trash')

    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 10 * 1000
    delay.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    trash_event = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash
    jdbc_multitable_consumer >= delay
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


# SDC-10987: JDBC Multitable Consumer multiple offset columns with initial offset
@database
def test_jdbc_multitable_consumer_initial_offset_at_the_end(sdc_builder, sdc_executor, database):
    """
    Set initial offset at the end of the table and verify that no records were read.
    """
    table_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": ["id"],
        "offsetColumnToInitialOffsetValue": [{
            "key": "id",
            "value": "5"
        }]
    }]

    trash = builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

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


# SDC-11324: JDBC MultiTable origin can create duplicate offsets
@database('mysql')
def test_jdbc_multitable_duplicate_offsets(sdc_builder, sdc_executor, database):
    """Validate that we will not create duplicate offsets. """
    table_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name}]
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


# SDC-11326: JDBC MultiTable origin forgets offset of non-incremental table on consecutive execution
@database('mysql')
@sdc_min_version('3.0.0.0')
def test_jdbc_multitable_lost_nonincremental_offset(sdc_builder, sdc_executor, database):
    """Validate the origin does not loose non-incremental offset on various runs."""
    table_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name, "enableNonIncremental": True}]
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


@sdc_min_version('3.9.0')
@database('oracle')
def test_jdbc_multitable_oracle_split_by_timestamp_with_timezone(sdc_builder, sdc_executor, database):
    """Make sure that we can properly partition TIMESTAMP WITH TIMEZONE type."""
    table_name = get_random_string(string.ascii_uppercase, 20)
    table_name_dest = get_random_string(string.ascii_uppercase, 20)

    connection = database.engine.connect()

    comparing_query = f"""(
        select * from {table_name}
        minus
        select * from {table_name_dest}
    ) union (
        select * from {table_name_dest}
        minus
        select * from {table_name}
    )"""

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                ID number primary key,
                TZ timestamp(6) with time zone
            )
        """)
        # Create destination table
        connection.execute(f"""CREATE TABLE {table_name_dest} AS SELECT * FROM {table_name} WHERE 1=0""")

        # Insert a few rows
        for m in range(0, 5):
            for s in range(0, 59):
                connection.execute(f"INSERT INTO {table_name} VALUES({m*100+s}, TIMESTAMP'2019-01-01 10:{m}:{s}-5:00')")
        connection.execute("commit")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        origin.table_configs = [{
            "tablePattern": f'%{table_name}%',
            "overrideDefaultOffsetColumns": True,
            "offsetColumns": ["TZ"],
            "enableNonIncremental": False,
            "partitioningMode": "REQUIRED",
            "partitionSize": "30",
            "maxNumActivePartitions": -1
        }]
        origin.number_of_threads = 2
        origin.maximum_pool_size = 2
        origin.max_batch_size_in_records = 30

        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

        FIELD_MAPPINGS = [dict(field='/ID', columnName='ID'),
                          dict(field='/TZ', columnName='TZ')]
        destination = builder.add_stage('JDBC Producer')
        destination.set_attributes(default_operation='INSERT',
                                   table_name=table_name_dest,
                                   field_to_column_mapping=FIELD_MAPPINGS,
                                   stage_on_record_error='STOP_PIPELINE')

        origin >> destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

        # Insert few more rows and validate the outcome again
        for m in range(6, 8):
            for s in range(0, 59):
                connection.execute(f"INSERT INTO {table_name} VALUES({m*100+s}, TIMESTAMP'2019-01-01 10:{m}:{s}-5:00')")
        connection.execute("commit")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

    finally:
        logger.info('Dropping table %s and %s in %s database ...', table_name, table_name_dest, database.type)
        connection.execute(f"DROP TABLE {table_name}")
        connection.execute(f"DROP TABLE {table_name_dest}")


# SDC-12404: Pipeline Stuck in Running State on JDBC MultiTable exception (Exception not propagated/Pipeline Hangs)
@database('oracle')
def test_jdbc_multitable_oracle_split_by_date(sdc_builder, sdc_executor, database):
    """Make sure that we can properly partition DATE type.
    More precisely, we want to run this pipeline:

    multitable >> jdbc
    multitable >= finisher

    With more than one thread and using a DATE column as a offset column.
    """
    table_name = get_random_string(string.ascii_uppercase, 20)
    table_name_dest = get_random_string(string.ascii_uppercase, 20)

    connection = database.engine.connect()

    comparing_query = f"""(
        select * from {table_name}
        minus
        select * from {table_name_dest}
    ) union (
        select * from {table_name_dest}
        minus
        select * from {table_name}
    )"""

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                ID number primary key,
                DT date
            )
        """)
        # Create destination table
        connection.execute(f"""CREATE TABLE {table_name_dest} AS SELECT * FROM {table_name} WHERE 1=0""")

        # Insert a few rows
        for m in range(0, 5):
            for s in range(0, 59):
                identifier = 100 * m + s
                connection.execute(
                    f"INSERT INTO {table_name} VALUES({identifier}, DATE'{_get_date_from_days(identifier)}')"
                )
        connection.execute("commit")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        # Partition size is set to 259200000 which corresponds to 30 days in ms,
        # since dates are translated to timestamps
        origin.table_configs = [{
            "tablePattern": f'%{table_name}%',
            "overrideDefaultOffsetColumns": True,
            "offsetColumns": ["DT"], # Should cause SDC < 3.11.0 to throw an UnsupportedOperationException
            "enableNonIncremental": False,
            "partitioningMode": "REQUIRED",
            "partitionSize": "259200000", # 30 days = 30*24*60*60*1000 (259200000)ms
            "maxNumActivePartitions": 2
        }]
        origin.number_of_threads = 2
        origin.maximum_pool_size = 2

        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

        FIELD_MAPPINGS = [dict(field='/ID', columnName='ID'),
                          dict(field='/DT', columnName='DT')]
        destination = builder.add_stage('JDBC Producer')
        destination.set_attributes(default_operation='INSERT',
                                   table_name=table_name_dest,
                                   field_to_column_mapping=FIELD_MAPPINGS,
                                   stage_on_record_error='STOP_PIPELINE')

        origin >> destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

        # Insert few more rows and validate the outcome again
        for m in range(6, 8):
            for s in range(0, 59):
                identifier = 100 * m + s
                connection.execute(
                    f"INSERT INTO {table_name} VALUES({identifier}, DATE'{_get_date_from_days(identifier)}')"
                )
        connection.execute("commit")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

    finally:
        logger.info('Dropping table %s and %s in %s database ...', table_name, table_name_dest, database.type)
        connection.execute(f"DROP TABLE {table_name}")
        connection.execute(f"DROP TABLE {table_name_dest}")


@sdc_min_version('3.9.0')
@database('mysql')
def test_jdbc_multitable_consumer_origin_high_resolution_timestamp_offset(sdc_builder, sdc_executor, database):
    """
    Check if Jdbc Multi-table Origin can retrieve any records from a table using as an offset a high resolution
    timestamp of milliseconds order. It is checked that the records read have a timestamp greater than the timestamp
    used as initial offset.

    Pipeline looks like:

    jdbc_multitable_consumer >> wiretap
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{'tablePattern': f'%{src_table_prefix}%',
                                                            'overrideDefaultOffsetColumns': True,
                                                            'offsetColumns': ['added'],
                                                            'offsetColumnToInitialOffsetValue': [{
                                                                'key': 'added',
                                                                'value': '${time:extractNanosecondsFromString(' +
                                                                         '"1996-12-02 00:00:00.020111000")}'
                                                            }]
                                                            }])

    wiretap = pipeline_builder.add_wiretap()
    jdbc_multitable_consumer >> wiretap.destination

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


# SDC-10053: Jdbc Multitable does not handle large gap in primary keys
@database
@sdc_min_version('3.0.0.0')
def test_jdbc_multitable_consumer_partitioned_large_offset_gaps(sdc_builder, sdc_executor, database):
    """
    Ensure that the multi-table JDBC origin can handle large gaps between offset columns in partitioned mode
    The destination is wiretap, and there is a finisher waiting for the no-more-data event

    The pipeline will be started, and we will capture the information retrieved using a wiretap,
    then we assert those captured rows match the expected data.
    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{
        "tablePattern": f'{table_name}',
        "enableNonIncremental": False,
        "partitioningMode": "REQUIRED",
        "partitionSize": "1000000",
        "maxNumActivePartitions": -1
    }])

    wiretap = pipeline_builder.add_wiretap()
    jdbc_multitable_consumer >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']
    jdbc_multitable_consumer >= finisher

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


@sdc_min_version('3.12.0')
@database('sqlserver')
@pytest.mark.parametrize('on_unknown_type_action', ['CONVERT_TO_STRING', 'STOP_PIPELINE'])
def test_jdbc_sqlserver_on_unknown_type_action(sdc_builder, sdc_executor, database, on_unknown_type_action):
    """Test JDBC Multitable Consumer with MS-SQL server for the on_unknown_type action.
        This is to verify SDC-12764.
        When the 'On Unknown Type' action is set to STOP_PIPELINE,the pipeline should stop with a StageException Error since it cannot convert DATETIMEOFFSET field
        When the 'On Unknown Type' action is set to CONVERT_TO_STRING, the pipeline should convert the unknown type to string and process next record
        The pipeline will look like:
            JDBC_Multitable_Consumer >> wiretap
    """

    if Version(sdc_executor.version) >= Version('3.14.0'):
        pytest.skip("Skipping SQLServer Unknown Type action check, since DATETIMEOFFSET field is now natively supported from SDC Version 3.14.0")

    column_type = 'DATETIMEOFFSET'
    INPUT_DATE = "'2004-05-23T14:25:10'"
    EXPECTED_OUTCOME = OrderedDict(id=1, date_offset='2004-05-23 14:25:10 +00:00')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Setup Origin with specified unknown type action
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{table_name}%'}],
                                            on_unknown_type=on_unknown_type_action)

    # Setup destination
    wiretap=pipeline_builder.add_wiretap()

    # Connect the pipeline stages
    jdbc_multitable_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    # Create table and add a row
    connection.execute(f"""
        CREATE TABLE {table_name}(
            id int primary key,
            date_offset {column_type} NOT NULL
        )
    """)
    connection.execute(f"INSERT INTO {table_name} VALUES(1, {INPUT_DATE})")

    try:
        if on_unknown_type_action == 'STOP_PIPELINE':
            # Pipeline should stop with StageException
            with pytest.raises(Exception):
                sdc_executor.start_pipeline(pipeline)
                sdc_executor.stop_pipeline(pipeline)

            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert 'RUN_ERROR' == status
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
            output_records = wiretap.output_records
            assert len(output_records) == 1
            assert output_records[0].field == EXPECTED_OUTCOME

    finally:
        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if status == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.14.0')
@database('sqlserver')
def test_jdbc_sqlserver_datetimeoffset_as_primary_key(sdc_builder, sdc_executor, database):
    """Test JDBC Multitable Consumer with SQLServer table configured with DATETIMEOFFSET column as primary key.
        The pipeline will look like:
            JDBC_Multitable_Consumer >> wiretap
    """
    INPUT_COLUMN_TYPE, INPUT_DATE = 'DATETIMEOFFSET', "'2004-05-23 14:25:10.3456 -08:00'"
    EXPECTED_TYPE, EXPECTED_VALUE = 'ZONED_DATETIME', '2004-05-23T14:25:10.3456-08:00'

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{table_name}%'}])

    wiretap=pipeline_builder.add_wiretap()

    jdbc_multitable_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    connection.execute(f"""
        CREATE TABLE {table_name}(
            dto {INPUT_COLUMN_TYPE} NOT NULL PRIMARY KEY
        )
    """)
    connection.execute(f"INSERT INTO {table_name} VALUES({INPUT_DATE})")

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        record = wiretap.output_records[0]

        assert record.field['dto'].type == EXPECTED_TYPE
        assert record.field['dto'].value == EXPECTED_VALUE

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


# Test for SDC-13288
@database('db2')
def test_jdbc_producer_db2_long_record(sdc_builder, sdc_executor, database):
    """Test that JDBC Producer correctly sends record when setting Custom Data SQLSTATE for db2 database instead of
     throwing StageException. The pipelines reads a file with 5 records 1 by 1 having the last record being biggest
     than the db2 table column size. That throws an error with an specific SQL Code (22001). Having that code in Custom
     Data SQLSTATE sends the last record to error.

     The pipeline looks like:

     directory_origin >> [jdbc_producer, wiretap]

     In order to create the file read by directory origin another pipeline is used that looks like:

     dev_raw_data_source >> local_fs
    """

    # Insert data into file.
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = ['1,hello', '2,hello', '3,hello', '4,hello', '5,hellolargerword']
    _setup_delimited_file(sdc_executor, tmp_directory, csv_records)

    # Create directory origin.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             batch_size_in_recs=1)

    # Create jdbc producer destination.
    # Create table. db2 internal sets table name in uppercase. Thus using directly ascii uppercase.
    table_name = get_random_string(string.ascii_uppercase, 20)
    database.engine.execute(f'CREATE TABLE {table_name} (id VARCHAR(20) NOT NULL PRIMARY KEY, a VARCHAR(10));')
    field_to_column_mapping = [dict(columnName='ID',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/0',
                                    paramValue='?'),
                               dict(columnName='A',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/1',
                                    paramValue='?')]
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation="INSERT",
                                 schema_name=DEFAULT_DB2_SCHEMA,
                                 table_name=table_name,
                                 field_to_column_mapping=field_to_column_mapping,
                                 stage_on_record_error='TO_ERROR',
                                 data_sqlstate_codes=["22001"])

    wiretap = pipeline_builder.add_wiretap()
    directory >> [jdbc_producer, wiretap.destination]

    directory_jdbc_producer_pipeline = pipeline_builder.build(
        title='Directory - JDBC Producer. Test DB2 sql code error').configure_for_environment(database)
    sdc_executor.add_pipeline(directory_jdbc_producer_pipeline)

    try:
        sdc_executor.start_pipeline(directory_jdbc_producer_pipeline).wait_for_pipeline_output_records_count(4)
        sdc_executor.stop_pipeline(directory_jdbc_producer_pipeline)

        result = database.engine.execute(f'SELECT ID,A FROM {table_name};')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # Order by id.
        result.close()

        # Assert records in database include from id=1 to id=4 excluding id=5. Columns => record[0] = id, record[1] = a.
        assert data_from_database == [(record[0], record[1]) for record in
                                      [unified_record.split(',') for unified_record in csv_records[:-1]]]

        error_msgs = wiretap.error_records
        assert 1 == len(error_msgs)

        error_record = error_msgs[0]

        assert 'hellolargerword' == error_record.field['1']
        assert 'JDBC_14' == error_record.header['errorCode']
        assert 'SQLSTATE=22001' in error_record.header['errorMessage']

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        database.engine.execute(f'DROP TABLE {table_name}')


# SDC-13624:  JDBC Multitable Consumer ingests duplicates when initial offset is set for a column in partitioned mode
@database
@sdc_min_version('3.0.0.0')
def test_jdbc_multitable_consumer_duplicates_read_when_initial_offset_configured(sdc_builder, sdc_executor, database):
    """
    SDC-13625 Integration test for SDC-13624 - MT Consumer ingests duplicates when initial offset is specified
    Setup origin as follows:
        partitioning enabled + num_threads and num partitions > 1 + override offset column set
        + initial value specified for offset

    Verify that origin does not ingest the records more than once (duplicates) when initial value for offset is set

    Pipeline:
        JDBC MT Consumer >> Wiretap
                         >= Pipeline Finisher (no-more-data)
    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{
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

    jdbc_multitable_consumer.number_of_threads = 2
    jdbc_multitable_consumer.maximum_pool_size = 2

    wiretap = pipeline_builder.add_wiretap()
    jdbc_multitable_consumer >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']
    jdbc_multitable_consumer >= finisher

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


# SDC-14489: JDBC Multitable origin must escape string columns
@database
@pytest.mark.parametrize('input_string', [
    "::problematical::value::",
    "=another=problematical=value=",
    ""
])
def test_multitable_string_offset_column(sdc_builder, sdc_executor, database, input_string):
    """Ensure that problematical values in String-typed offset column are covered, e.g. our special separator '::'."""
    if database.type == 'Oracle' and not input_string:
        pytest.skip("Oracle doesn't support concept of empty string - it always converts it to NULL which is invalid for primary key.")

    builder = sdc_builder.get_pipeline_builder()
    table_name = get_random_string(string.ascii_letters, 10)

    origin = builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": f'{table_name}'}]
    origin.max_batch_size_in_records = 10

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'{table_name}'

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
        assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.errorRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.stageErrors.counter').count == 0

        # And verify that we properly read that one record
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].get_field_data('/id') == input_string
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
@pytest.mark.parametrize('batch_strategy', ['SWITCH_TABLES', 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE'])
@pytest.mark.parametrize('no_of_threads', [1, 2, 3])
def test_jdbc_multitable_consumer_batch_strategy(sdc_builder, sdc_executor, database, batch_strategy, no_of_threads):
    """
    Check if Jdbc Multi-table Origin can load a couple of tables without duplicates using both batch_strategy options.
    Also it includes different thread combinations to ensure that with less, same and more threads works fine.

    jdbc_multitable_consumer >> wiretap.destination
    jdbc_multitable_consumer >= pipeline_finished_executor
    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")
    no_of_records_per_table = random.randint(5001, 10000)

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name1 = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'
    table_name2 = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'{src_table_prefix}%'}],
                                            per_batch_strategy=batch_strategy,
                                            number_of_threads=no_of_threads,
                                            maximum_pool_size=no_of_threads)

    pipeline_finished_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = pipeline_builder.add_wiretap()

    jdbc_multitable_consumer >> wiretap.destination
    jdbc_multitable_consumer >= pipeline_finished_executor

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

@sdc_min_version('5.4.0')
@database
@pytest.mark.parametrize('batch_strategy', ['SWITCH_TABLES', 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE'])
@pytest.mark.parametrize('max_tables', [1, 2])
def test_jdbc_multitable_consumer_max_tables(sdc_builder, sdc_executor, database, batch_strategy, max_tables):
    """
    Check if Jdbc Multi-table Origin validates properly when 'switch_tables' is set and there are more table to read from
    than 'max_tables'.

    jdbc_multitable_consumer >> trash
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name1 = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'
    table_name2 = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'{src_table_prefix}%'}],
                                            per_batch_strategy=batch_strategy,
                                            maximum_number_of_tables=max_tables)

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    def get_table_schema(table_name):
        return sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('field1', sqlalchemy.String(32))
        )

    table1 = get_table_schema(table_name1)
    table2 = get_table_schema(table_name2)

    try:
        logger.info('Creating table %s in %s database ...', table_name1, database.type)
        logger.info('Creating table %s in %s database ...', table_name2, database.type)
        table1.create(database.engine)
        table2.create(database.engine)

        logger.info('Validating pipeline configuration...')
        if batch_strategy == 'SWITCH_TABLES' and max_tables == 1:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)
            assert e is not None
            assert e.value.issues is not None
            assert e.value.issues['issueCount'] == 1
            assert 'JDBC_205' in e.value.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            logger.info('Validation correctly failed for max tables == 1')
        else:
            sdc_executor.validate_pipeline(pipeline)

    finally:
        logger.info('Dropping table %s in %s database...', table_name1, database.type)
        logger.info('Dropping table %s in %s database...', table_name2, database.type)
        table1.drop(database.engine)
        table2.drop(database.engine)


@database
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

    JDBC Multitable Consumer >> Splitter >> Wiretap1 (12000 records)
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
            'table_configs': [{
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

        jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
        jdbc_multitable_consumer.set_attributes(**attributes)

        stream_selector = pipeline_builder.add_stage('Stream Selector')

        jdbc_multitable_consumer >> stream_selector

        event_wiretap = pipeline_builder.add_wiretap()
        jdbc_multitable_consumer >= event_wiretap.destination

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
        jdbc_multitable_consumer.table_configs[0]["tablePattern"] = f'{table_prefix}%'

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


# COLLECTOR-333: JDBC Multitable Consumer cannot use wildcard (%) in Schema selection
# COLLECTOR-534: Test new MySQL schema validator
@database
@sdc_min_version('4.4.0')
@pytest.mark.parametrize('schema_value', ['', '%', '{database}'])
def test_jdbc_schema_settings(sdc_builder, sdc_executor, database, schema_value):
    """This test currently covers only the schema setting of table configs.  It was added
    to verify the fix for COLLECTOR-333 where, for mysql, setting the schema to '%'
    (wildcard) will now return a bespoke error message indicating that mysql does not
    support multiple schemas and so doesn't support a wildcard for schema selection.
    At the time of this writing, this is the only known jdbc database that doesn't
    support a wildcard for schema selection - but as our docs recommend using a
    wildcard for jdbc table configs, we want this test to run against all of our tested
    databases to test this "default" case.
    """

    # At the time of this writing, MySQL/MariaDB has the requirement that the schema be blank or
    # the same as the database
    dbs_with_this_behavior = {'MySQL', 'MariaDB'}

    # MySQL/MariDB gives a default schema to a database - so the test case where the default
    # schema name is automatically the database name is only applicable to MySQL/MariaDB
    if schema_value == '{database}' and database.type not in dbs_with_this_behavior:
        logger.info('skipping database as schema test for database type ' + database.type)
        pytest.skip('database as schema test not valid for database type ' + database.type + ' ...skipping')

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('JDBC Multitable Consumer')
    trash = builder.add_stage('Trash')
    origin >> trash

    table_name = 'uniq_tablename_' + get_random_string(string.ascii_lowercase, 10)

    # Set the schema config
    if schema_value == '{database}':
        # here, we assume that the schema name is exactly the same as the database name
        origin.table_configs = [{'schema': database.database}]
    else:
        origin.table_configs = [{'schema': schema_value, 'tablePattern':f'{table_name}'}]

    pipeline = builder.build().configure_for_environment(database)

    # some sugar for debugging this test
    if 'schema' in origin.table_configs[0].keys():
        logger.info('DB: ' + database.type + "; Table config schema: '" + origin.table_configs[0]['schema'] + "'")
    else:
        logger.info('DB: ' + database.type + '; Table config schema: [empty]')

    rows_in_database = [{'id': row['id'], 'NAME': row['name']} for row in ROWS_IN_DATABASE]
    columns = [
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    ]
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata, *columns)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.add_pipeline(pipeline)
        logger.info('Validating pipeline configuration...')
        if database.type in dbs_with_this_behavior and schema_value == '%':
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)
            assert e is not None
            assert e.value.issues is not None
            assert e.value.issues['issueCount'] == 1
            assert 'JDBC_104' in e.value.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            logger.info('Validation correctly failed for wildcard schema on ' + database.type + ' database')
        else:
            sdc_executor.validate_pipeline(pipeline)

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('5.2.0')
@database
def test_jdbc_primary_keys_headers(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"

    INPUT_DATA = [
        {'id': 1, 'name': 'The Lich King', 'game': 'World of Warcraft'},
        {'id': 2, 'name': 'Bowser', 'game': 'Super Mario Bros'},
        {'id': 3, 'name': 'Handsome Jack', 'game': 'Borderlands 2'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name}]

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

            if database.type is 'Oracle':
                primary_key_specification_expected = PRIMARY_KEY_ORACLE_TABLE
            elif database.type is 'SQLServer':
                primary_key_specification_expected = PRIMARY_KEY_SQLSERVER_TABLE
            elif database.type is 'PostgreSQL':
                primary_key_specification_expected = PRIMARY_KEY_POSTGRESQL_TABLE
            elif database.type is 'MariaDB':
                primary_key_specification_expected = PRIMARY_KEY_MARIADB_TABLE
            elif database.type is 'MySQL':
                if Version(database.version) < Version('8.0.0'):
                    primary_key_specification_expected = PRIMARY_KEY_MYSQL_PRE_V8_TABLE
                else:
                    primary_key_specification_expected = PRIMARY_KEY_MYSQL_TABLE
            else:
                pytest.fail(f"Unsupported database type: {database.type}")

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


@sdc_min_version('5.2.0')
@database
def test_jdbc_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"

    pipeline_builder = sdc_builder.get_pipeline_builder()

    if database.type is 'Oracle':
        offsetCol = "MY_NUMBER_1"
    elif database.type is 'PostgreSQL':
        offsetCol = "my_integer"
    else:
        offsetCol = "my_int"

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name, "enableNonIncremental": True,
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

        if database.type is 'Oracle':
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_ORACLE
        elif database.type is 'SQLServer':
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER
        elif database.type is 'PostgreSQL':
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_POSTGRESQL
        elif database.type is 'MariaDB':
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_MARIADB
        elif database.type is 'MySQL':
            if Version(database.version) < Version('8.0.0'):
                primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_MYSQL_PRE_V8
            else:
                primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_MYSQL
        else:
            pytest.fail(f"Unsupported database type: {database.type}")

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table {table_name}')


@sdc_min_version('5.2.0')
@database
def test_jdbc_non_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"

    pipeline_builder = sdc_builder.get_pipeline_builder()

    if database.type is 'Oracle':
        offsetCol = "MY_DATE"
    else:
        offsetCol = "my_date"

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name, "enableNonIncremental": True,
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

        if database.type is 'Oracle':
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_ORACLE
        elif database.type is 'SQLServer':
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER
        elif database.type is 'PostgreSQL':
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_POSTGRESQL
        elif database.type is 'MariaDB':
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_MARIADB
        elif database.type is 'MySQL':
            if Version(database.version) < Version('8.0.0'):
                primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL_PRE_V8
            else:
                primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL
        else:
            pytest.fail(f"Unsupported database type: {database.type}")

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table {table_name}')


@sdc_min_version('5.2.0')
@database
def test_jdbc_vendor_header(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_lowercase, 10)
    vendor_specification = f"jdbc.vendor"

    INPUT_DATA = [
        {'id': 1, 'name': 'The Lich King', 'game': 'World of Warcraft'},
        {'id': 2, 'name': 'Bowser', 'game': 'Super Mario Bros'},
        {'id': 3, 'name': 'Handsome Jack', 'game': 'Borderlands 2'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name}]

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

            if database.type is 'Oracle':
                vendor_metadata_expected = 'Oracle'
            elif database.type is 'SQLServer':
                vendor_metadata_expected = 'Microsoft SQL Server'
            elif database.type is 'PostgreSQL':
                vendor_metadata_expected = 'PostgreSQL'
            elif database.type is 'MariaDB':
                vendor_metadata_expected = 'MariaDB'
            else:
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

