# Copyright 2024 StreamSets Inc.
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
import uuid

import pytest
import sqlalchemy

from sqlalchemy import Numeric, Date
from streamsets.sdk.exceptions import ValidationError
from streamsets.sdk.exceptions import StartError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from stage.utils.utils_primary_key_metadata import PRIMARY_KEY_NON_NUMERIC_METADATA_ORACLE,  \
    PRIMARY_KEY_NUMERIC_METADATA_ORACLE, \
    PRIMARY_KEY_ORACLE_TABLE, \
    get_create_table_query_non_numeric, get_create_table_query_numeric, get_insert_query_non_numeric, \
    get_insert_query_numeric

logger = logging.getLogger(__name__)

pytestmark = [database("oracle"), sdc_min_version('5.11.0')]

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima', 'leaves': 4.5, 'doj': datetime.date(2012, 9, 9), 'age': '1'},
    {'id': 2, 'name': 'Jarcec', 'leaves': 5.5, 'doj': datetime.date(2016, 10, 12), 'age': '100'},
    {'id': 3, 'name': 'Arvind', 'leaves': 5.0, 'doj': datetime.date(2018, 1, 2), 'age': '2'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie'},
    {'id': 4, 'name': 'Jarcec'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

DEFAULT_DB2_SCHEMA = 'DB2INST1'


# SDC-11009: Run away pipeline runners in Oracle Multithread origins when no-more-data generation delay is configured
def test_oracle_multitable_consumer_with_no_more_data_event_generation_delay(sdc_builder, sdc_executor, database):
    """
    Make sure that when a delayed no-more-data is being processed, the pipeline properly waits on the processing to
    finish before stopping.

    source >> trash
           >= delay (only for no-more-data) >> trash
    """
    src_table = get_random_string(string.ascii_uppercase, 6)
    connection = None

    pipeline_builder = sdc_builder.get_pipeline_builder()
    oracle_multitable_consumer = pipeline_builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.no_more_data_event_generation_delay_in_seconds = 1
    oracle_multitable_consumer.table_configs = [{"tablePattern": f'%{src_table}%'}]

    trash = pipeline_builder.add_stage('Trash')

    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 10 * 1000
    delay.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    trash_event = pipeline_builder.add_stage('Trash')

    oracle_multitable_consumer >> trash
    oracle_multitable_consumer >= delay
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
            logger.info('Dropping table %s in %s database...', src_table, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()


# SDC-5098: Multi Table Oracle: Specifying invalid type for initial offset leads to NumberFormatException
def test_oracle_multitable_consumer_invalid_initial_offset(sdc_builder, sdc_executor, database):
    """
    Set initial invalid offset and verify error is thrown
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": ["id"],
        "offsetColumnToInitialOffsetValue": [{
            "key": "id",
            "value": str(uuid.uuid4())  # invalid offset
        }]
    }]

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

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
        # JDBC_INIT_25 is the error code expected here for versions above 5.8.0
        expected_error = 'JDBC_INIT_25'
        with pytest.raises(Exception) as error:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        assert expected_error in error.value.message, \
            f'Expected a {expected_error} error, got "{error.value.message}" instead'

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()


# SDC-10987: Oracle Multitable Consumer multiple offset columns with initial offset
def test_oracle_multitable_consumer_initial_offset_at_the_end(sdc_builder, sdc_executor, database):
    """
    Set initial offset at the end of the table and verify that no records were read.
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": ["id"],
        "offsetColumnToInitialOffsetValue": [{
            "key": "id",
            "value": "5"
        }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

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
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()


@pytest.mark.parametrize('error_code, offset_column, initial_offset, last_offset',
                         [
                             ('JDBC_INIT_26', 'age', '2', '${missing_close_bracket'),
                             ('JDBC_INIT_43', 'age', '2', '100'),
                             ('JDBC_INIT_43', 'id', '2', '1'),
                             ('JDBC_INIT_43', 'id', '2', '-5'),
                             ('JDBC_INIT_43', 'leaves', '5.5', '5.0'),
                             ('JDBC_INIT_43', 'leaves', '1.0', '-5.5'),
                             ('JDBC_INIT_43', 'doj', '1378987751000', '1284293351000'),
                             ('JDBC_INIT_43', 'doj', '1284293351000', '-1284111351000')
                         ]
                         )
def test_oracle_multitable_consumer_invalid_offset_configuration(
        sdc_builder,
        sdc_executor,
        database,
        error_code,
        offset_column,
        initial_offset,
        last_offset
):
    """
    Set last offset less than initial offset and verify that a StartError occurs.
    Should not test with a String offset value as it is non-partitionable.
    Tested over String just to verify that it throws an error when invalid offset values are given,
    even though the last offset would not be considered while fetching the records from the database.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": [offset_column],
        "partitioningMode": "BEST_EFFORT",
        "maxNumActivePartitions": -1,
        "partitionSize": "5",
        "offsetColumnToInitialOffsetValue": [{
            "key": offset_column,
            "value": initial_offset
        }],
        "offsetColumnToLastOffsetValue": [{
            "key": offset_column,
            "value": last_offset
        }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
        sqlalchemy.Column('leaves', Numeric(precision=10, scale=2), quote=True),
        sqlalchemy.Column('doj', Date, quote=True),
        sqlalchemy.Column('age', sqlalchemy.String(32), quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(StartError) as error:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('offset_column, initial_offset, last_offset, partition_size',
                         [
                             ('age', '100', '2', '5'),
                             ('id', '1', '2', '5'),
                             ('id', '3', '100', '5'),
                             ('id', '-5', '2', '5'),
                             ('leaves', '4.5', '5.0', '5'),
                             ('leaves', '5.5', '100.0', '5'),
                             ('leaves', '-20.0', '5.0', '5'),
                             ('doj', '1347192551000', '1476274151000', '31556926000'),
                             ('doj', '1514764800000', '1914894951000', '31556926000'),
                             ('doj', '-1284111351000', '1476144000000', '31556926000')
                         ]
                         )
def test_oracle_multitable_consumer_valid_offset_configuration(sdc_builder, sdc_executor, database, offset_column,
                                                               initial_offset, last_offset, partition_size):
    """
    Set initial offset less than last offset and verify that correct number of records are read.
    Should not test with a String offset value as it is non-partitionable.
    Valid Till 5.7.1 --> Tested over String just to verify that it doesn't throw error when valid offset values are
    given, even though the last offset would not be considered while fetching the records from the database.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": [offset_column],
        "partitioningMode": "BEST_EFFORT",
        "maxNumActivePartitions": -1,
        "partitionSize": partition_size,
        "offsetColumnToInitialOffsetValue": [{
            "key": offset_column,
            "value": initial_offset
        }],
        "offsetColumnToLastOffsetValue": [{
            "key": offset_column,
            "value": last_offset
        }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
        sqlalchemy.Column('leaves', Numeric(precision=10, scale=2), quote=True),
        sqlalchemy.Column('doj', Date, quote=True),
        sqlalchemy.Column('age', sqlalchemy.String(32), quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)
        sdc_executor.add_pipeline(pipeline)

        if offset_column == 'age' or offset_column == 'name':
            with pytest.raises(Exception) as error:
                sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

            error_code = 'JDBC_INIT_40'
            assert error_code in error.value.message, \
                f'Expected a {error_code} error, got "{error.value.message}" instead'
        else:
            sdc_executor.start_pipeline(pipeline)

            # We simply wait for the records to be read
            time.sleep(5)

            sdc_executor.stop_pipeline(pipeline)

            # There must be 1 record read
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1

    finally:

        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('offset_column1, initial_offset1, last_offset1, offset_column2, initial_offset2, last_offset2',
                         [
                             ('id', '1', '2', 'leaves', '4.5', '5.0'),
                             ('id', '1', '3', 'age', '100', '2')
                         ]
                         )
def test_oracle_multitable_consumer_multiple_initial_last_offset(sdc_builder, sdc_executor, database, offset_column1,
                                                                 initial_offset1, last_offset1, offset_column2,
                                                                 initial_offset2, last_offset2):
    """
    Set multiple offsets to verify that it throws a validation error as the table is not partitionable.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": [offset_column1, offset_column2],
        "partitioningMode": "BEST_EFFORT",
        "maxNumActivePartitions": -1,
        "partitionSize": '5',
        "offsetColumnToInitialOffsetValue": [{
            "key": offset_column1,
            "value": initial_offset1
        },
        {
            "key": offset_column2,
            "value": initial_offset2
        }],
        "offsetColumnToLastOffsetValue": [{
            "key": offset_column1,
            "value": last_offset1
        },
        {
            "key": offset_column2,
            "value": last_offset2
        }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
        sqlalchemy.Column('leaves', Numeric(precision=10, scale=2), quote=True),
        sqlalchemy.Column('doj', Date, quote=True),
        sqlalchemy.Column('age', sqlalchemy.String(32), quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        with pytest.raises(Exception) as error:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        error_code = 'JDBC_INIT_40'
        assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'

    finally:

        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('offset_column1, last_offset1, offset_column2, last_offset2',
                         [
                             ('id', '2', 'leaves', '5.0'),
                             ('id', '3', 'age', '2')
                         ]
                         )
def test_oracle_multitable_consumer_multiple_last_offset_only(sdc_builder, sdc_executor, database, offset_column1,
                                                              last_offset1, offset_column2, last_offset2):
    """
    Set multiple last offsets only to verify that it throws a validation error as the table is not partitionable.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": [offset_column1, offset_column2],
        "partitioningMode": "BEST_EFFORT",
        "maxNumActivePartitions": -1,
        "partitionSize": '5',
        "offsetColumnToLastOffsetValue": [{
            "key": offset_column1,
            "value": last_offset1
        },
            {
                "key": offset_column2,
                "value": last_offset2
            }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
        sqlalchemy.Column('leaves', Numeric(precision=10, scale=2), quote=True),
        sqlalchemy.Column('doj', Date, quote=True),
        sqlalchemy.Column('age', sqlalchemy.String(32), quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        with pytest.raises(Exception) as error:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        error_code = 'JDBC_INIT_40'
        assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'

    finally:

        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


def test_oracle_multitable_consumer_invalid_last_offset(sdc_builder, sdc_executor, database):
    """
    Set last invalid offset and verify error is thrown
    Should not test with a String offset value as it is non-partitionable.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": ["id"],
        "offsetColumnToLastOffsetValue": [{
            "key": "id",
            "value": str(uuid.uuid4())  # invalid offset
        }]
    }]

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

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
        with pytest.raises(Exception) as error:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        error_code = 'JDBC_INIT_25'
        assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('offset_column, last_offset, partition_size',
                         [
                             ('id', '1', '5'),
                             ('id', '-1', '5'),
                             ('leaves', '4.5', '5'),
                             ('leaves', '-4.5', '5'),
                             ('doj', '1347148800000', '31556926000'),
                             ('doj', '-1347148800000', '31556926000')
                         ]
                         )
def test_oracle_multitable_consumer_last_offset_at_the_start(sdc_builder, sdc_executor, database,
                                                             offset_column, last_offset, partition_size):
    """
    Set last offset at the first record and verify that no records are read.
    Should not test with a String offset value as it is non-partitionable.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": [offset_column],
        "partitioningMode": "BEST_EFFORT",
        "maxNumActivePartitions": -1,
        "partitionSize": partition_size,
        "offsetColumnToLastOffsetValue": [{
            "key": offset_column,
            "value": last_offset
        }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
        sqlalchemy.Column('leaves', Numeric(precision=10, scale=2), quote=True),
        sqlalchemy.Column('doj', Date, quote=True)
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
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('offset_column, last_offset, expected_count, partition_size',
                         [
                             ('id', '3', 2, '5'),
                             ('id', '100', 3, '5'),
                             ('id', '-5', 0, '5'),
                             ('leaves', '5.5', 2, '5'),
                             ('leaves', '100.0', 3, '5'),
                             ('leaves', '-10.0', 0, '5'),
                             ('doj', '1514851200000', 2, '31556926000'),
                             ('doj', '1914894951000', 3, '31556926000'),
                             ('doj', '-1547148800000', 0, '31556926000')
                         ]
)
def test_oracle_multitable_consumer_last_offset_configuration(sdc_builder, sdc_executor, database, offset_column,
                                                              last_offset, expected_count, partition_size):
    """
    Set valid last offset and verify that records are read properly.
    Should not test with a String offset value as it is non-partitionable.
    The pipeline looks like :
            oracle_multitable_consumer >> trash
    """
    table_name = get_random_string(string.ascii_uppercase, 20)
    connection = None

    builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.table_configs = [{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": [offset_column],
        "partitioningMode": "BEST_EFFORT",
        "maxNumActivePartitions": -1,
        "partitionSize": partition_size,
        "offsetColumnToLastOffsetValue": [{
            "key": offset_column,
            "value": last_offset
        }]
    }]
    oracle_multitable_consumer.set_attributes(use_quoted_identifiers=True)

    trash = builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
        sqlalchemy.Column('leaves', Numeric(precision=10, scale=2), quote=True),
        sqlalchemy.Column('doj', Date, quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # We simply wait for the records to be read
        time.sleep(5)

        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == expected_count
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == expected_count

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()
        sdc_executor.remove_pipeline(pipeline)


def test_oracle_multitable_oracle_split_by_timestamp_with_timezone(sdc_builder, sdc_executor, database):
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

        origin = builder.add_stage('Oracle Multitable Consumer')
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
        if connection is not None:
            logger.info('Dropping table %s and %s in %s database ...', table_name, table_name_dest, database.type)
            connection.execute(f"DROP TABLE {table_name}")
            connection.execute(f"DROP TABLE {table_name_dest}")
            connection.close()


# SDC-12404: Pipeline Stuck in Running State on JDBC MultiTable exception (Exception not propagated/Pipeline Hangs)
def test_oracle_multitable_oracle_split_by_date(sdc_builder, sdc_executor, database):
    """Make sure that we can properly partition DATE type.
    More precisely, we want to run this pipeline:

    multitable >> oracle
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

        origin = builder.add_stage('Oracle Multitable Consumer')
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
        destination = builder.add_stage('Oracle')
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
        if connection is not None:
            logger.info('Dropping table %s and %s in %s database ...', table_name, table_name_dest, database.type)
            connection.execute(f"DROP TABLE {table_name}")
            connection.execute(f"DROP TABLE {table_name_dest}")
            connection.close()


def test_oracle_multitable_multithread(sdc_builder, sdc_executor, database):
    """Make sure that we can have multiple threads reading and writing:

    multitable >> jdbc
    multitable >= finisher

    """
    table_name_ori_pref = get_random_string(string.ascii_uppercase, 20)
    table_name_dest_pref = get_random_string(string.ascii_uppercase, 20)
    number_of_tables = 20
    number_of_rows = 20
    number_of_threads = 5
    maximum_pool_size = 5
    connection = database.engine.connect()

    try:
        # Create number_of_tables origin tables
        for i in range(number_of_tables):
            connection.execute(f"""
                CREATE TABLE {table_name_ori_pref}_{i}(
                    ID number primary key,
                    ST varchar(10),
                    TO_TABLE varchar(25)
                )
            """)
            # Create number_of_tables destination table
            connection.execute(f"""
                CREATE TABLE {table_name_dest_pref}_{i} 
                AS SELECT ID, ST FROM {table_name_ori_pref}_{i} WHERE 1=0
            """)
            # Insert number_of_rows rows in each table
            for s in range(number_of_rows):
                identifier = 100 * i + s
                connection.execute(f"""
                    INSERT INTO {table_name_ori_pref}_{i} 
                    VALUES({identifier}, 'hello-{identifier}', '{table_name_dest_pref}_{i}')
                """)
        connection.execute("commit")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Oracle Multitable Consumer')
        origin.table_configs = [{
            "tablePattern": f'{table_name_ori_pref}%',
            "overrideDefaultOffsetColumns": True,
            "offsetColumns": ["ID"],
            "enableNonIncremental": False
        }]
        origin.set_attributes(use_quoted_identifiers=True)

        origin.set_attributes(number_of_threads=number_of_threads, maximum_pool_size=maximum_pool_size)
        origin.set_attributes(maximum_number_of_tables=-1)

        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

        FIELD_MAPPINGS = [dict(field='/ID', columnName='ID', dataType='DECIMAL'),
                          dict(field='/ST', columnName='ST', dataType='STRING')]
        destination = builder.add_stage('JDBC Producer')
        destination.set_attributes(default_operation='INSERT',
                                   field_to_column_mapping=FIELD_MAPPINGS,
                                   stage_on_record_error='STOP_PIPELINE')

        origin >> destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)

        #Configure for environment capitalizes tablename value and makes the test fail
        destination.set_attributes(table_name="${record:value('/TO_TABLE')}")

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        for i in range(number_of_tables):
            result_origin = [row.items() for row in connection.execute(f'''
                Select ID,ST from {table_name_ori_pref}_{i}
            ''')]
            result_dest = [row.items() for row in connection.execute(f'''
                Select * from {table_name_dest_pref}_{i}
            ''')]
            assert result_origin == result_dest

    finally:
        if connection is not None:
            for i in range(number_of_tables):
                logger.info(
                    f'Dropping tables {table_name_ori_pref}_{i},{table_name_dest_pref}_{i} in database {database.type}')
                connection.execute(f"DROP TABLE {table_name_ori_pref}_{i}")
                connection.execute(f"DROP TABLE {table_name_dest_pref}_{i}")
            connection.close()


@pytest.mark.parametrize('table_name_pattern, table_exclusion_pattern, total_processed_tables', [
    ('#PREFIX#%', '', 10),  # no exclusion pattern
    ('#PREFIX#%', '.*', 0),  # exclude everything
    ('#PREFIX#TABLE%', '#PREFIX#TABLE[0-9]+', 5),  # exclude ending with numbers
    ('#PREFIX#TABLE%', '#PREFIX#TABLE1', 9),  # exclude table name as regex
    ('#PREFIX#TABLE%', '#PREFIX#TABLE1|#PREFIX#TABLE2', 8),  # exclude table using or regex
])
def test_oracle_multitable_consumer_table_exclusion(sdc_builder, sdc_executor, database,
                                                  table_name_pattern, table_exclusion_pattern, total_processed_tables):
    """
    Check if Oracle Multi-table Origin can load multiple tables without duplicates using multiple combinations of table
    patterns and exclusions.

    oracle_multitable_consumer >> wiretap.destination
    oracle_multitable_consumer >= pipeline_finished_executor
    """
    no_of_records_per_table = random.randint(51, 100)
    table_suffixes = ["TABLEA", "TABLEB", "TABLEC", "TABLED", "TABLEE",
                      "TABLE1", "TABLE2", "TABLE3", "TABLE4", "TABLE5"]
    num_tables = len(table_suffixes)

    src_table_prefix = get_random_string(string.ascii_uppercase, 6)
    table_names = [f'{src_table_prefix}{table_suffix}' for table_suffix in table_suffixes]
    connection = None

    table_name_pattern = table_name_pattern.replace("#PREFIX#", src_table_prefix)
    table_exclusion_pattern = table_exclusion_pattern.replace("#PREFIX#", src_table_prefix)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = pipeline_builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'{table_name_pattern}',
                                                              "tableExclusionPattern": f'{table_exclusion_pattern}'}])

    oracle_multitable_consumer.set_attributes(maximum_number_of_tables=-1)

    pipeline_finished_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = pipeline_builder.add_wiretap()

    oracle_multitable_consumer >> wiretap.destination
    oracle_multitable_consumer >= pipeline_finished_executor

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    def get_table_schema(table_name):
        return sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('FIELD1', sqlalchemy.String(32)),
            sqlalchemy.Column('FIELD2', sqlalchemy.String(32)),
        )

    def get_table_data(table):
        return [{'ID': i, 'FIELD1': f'field1_{i}_{table}', 'FIELD2': f'field2_{i}_{table}'}
                for i in range(1, no_of_records_per_table + 1)]

    tables = [get_table_schema(table_name) for table_name in table_names]
    table_datas = [get_table_data(i) for i in range(num_tables)]

    try:
        connection = database.engine.connect()
        for table, table_name, table_data in zip(tables, table_names, table_datas):
            logger.info('Creating table %s in %s database ...', table_name, database.type)
            table.create(database.engine)
            logger.info('Adding three rows into %s database ...', database.type)
            connection.execute(table.insert(), table_data)

        if total_processed_tables == 0:  # exclude everything
            with pytest.raises(Exception) as error:
                sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
            assert "JDBC_INIT_04" in error.value.message, f'Expected a JDBC_INIT_04 error, got "{error.value.message}" instead'

        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            records = [{'ID': record.field['ID'], 'FIELD1': record.field['FIELD1'], 'FIELD2': record.field['FIELD2']}
                       for record in wiretap.output_records]

            assert len(records) == no_of_records_per_table * total_processed_tables, "Wrong number of total records processed"
            for record in records:
                iterable = iter(record in table_data for table_data in table_datas)
                assert any(iterable), f'Wrong record found: "{record}"'
                assert not any(iterable), f'Duplicate record found: "{record}"'
    finally:
        for table, table_name in zip(tables, table_names):
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()


@pytest.mark.parametrize('batch_strategy', ['SWITCH_TABLES', 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE'])
@pytest.mark.parametrize('max_tables', [1, 2])
def test_oracle_multitable_consumer_max_tables(sdc_builder, sdc_executor, database, batch_strategy, max_tables):
    """
    Check if Oracle Multi-table Origin validates properly when 'switch_tables' is set and there are more table to read
    from than 'max_tables'.

    oracle_multitable_consumer >> trash
    """
    src_table_prefix = get_random_string(string.ascii_uppercase, 6)
    table_name1 = f'{src_table_prefix}_{get_random_string(string.ascii_uppercase, 20)}'
    table_name2 = f'{src_table_prefix}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    oracle_multitable_consumer = pipeline_builder.add_stage('Oracle Multitable Consumer')
    oracle_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'{src_table_prefix}%'}],
                                              per_batch_strategy=batch_strategy,
                                              maximum_number_of_tables=max_tables)

    trash = pipeline_builder.add_stage('Trash')

    oracle_multitable_consumer >> trash

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
            assert 'JDBC_205' in e.value.issues['stageIssues']['OracleMultitableConsumer_01'][0]['message']
            logger.info('Validation correctly failed for max tables == 1')
        else:
            sdc_executor.validate_pipeline(pipeline)

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

    Oracle Multitable Consumer >> Splitter >> Wiretap1 (12000 records)
                                Splitter >> Wiretap2 (36000 records)
                                Splitter >> Wiretap3 (6000 records)
                                Splitter >> Default Wiretap (0 records)
                             >= Event Wiretap

    """

    rows_in_tables = [12000, 36000, 6000]
    table_prefix = f'{get_random_string(string.ascii_uppercase, 6)}_'
    table_names = [f'{table_prefix}{get_random_string(string.ascii_uppercase, 20)}'
                   for _ in range(0, len(rows_in_tables))]
    tables = []
    all_rows = []
    pipeline = None
    connection = None

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
            'per_batch_strategy': test_data['per_batch_strategy'],
            'use_quoted_identifiers': True
        }
        oracle_multitable_consumer = pipeline_builder.add_stage('Oracle Multitable Consumer')
        oracle_multitable_consumer.set_attributes(**attributes)

        stream_selector = pipeline_builder.add_stage('Stream Selector')

        oracle_multitable_consumer >> stream_selector

        event_wiretap = pipeline_builder.add_wiretap()
        oracle_multitable_consumer >= event_wiretap.destination

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
                assert record.field['table'].value in table_names
                finished_tables.add(record.field['table'])
            elif record.header['values']['sdc.event.type'] == 'schema-finished':
                assert len(schema_finished_tables) == 0
                assert len(record.field['tables']) == len(table_names)
                for table in record.field['tables']:
                    assert table.value in table_names
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
        # for table in tables:
        if tables:
            for table in tables:
                table.drop(database.engine)
        if connection is not None:
            connection.close()


def test_oracle_primary_keys_headers(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_uppercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"
    connection = None

    INPUT_DATA = [
        {'id': 1, 'name': 'The Lich King', 'game': 'World of Warcraft'},
        {'id': 2, 'name': 'Bowser', 'game': 'Super Mario Bros'},
        {'id': 3, 'name': 'Handsome Jack', 'game': 'Borderlands 2'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('Oracle Multitable Consumer')
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

            primary_key_specification_json = json.dumps(
                json.loads(record.header.values[primary_key_specification]),
                sort_keys=True
            )

            primary_key_specification_expected_json = json.dumps(
                json.loads(PRIMARY_KEY_ORACLE_TABLE),
                sort_keys=True
            )

            assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()


def test_oracle_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_uppercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"
    connection = None

    pipeline_builder = sdc_builder.get_pipeline_builder()

    offset_col = "MY_NUMBER_1"

    origin = pipeline_builder.add_stage('Oracle Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name, "enableNonIncremental": True,
                             "overrideDefaultOffsetColumns": True,
                             "offsetColumns": [offset_col]}]

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

        primary_key_specification_expected_json = json.dumps(
            json.loads(PRIMARY_KEY_NUMERIC_METADATA_ORACLE),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        if connection is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f'drop table {table_name}')
            connection.close()


def test_oracle_non_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_uppercase, 10)
    primary_key_specification = f"jdbc.primaryKeySpecification"
    connection = None

    pipeline_builder = sdc_builder.get_pipeline_builder()

    offset_col = "MY_DATE"

    origin = pipeline_builder.add_stage('Oracle Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name, "enableNonIncremental": True,
                             "overrideDefaultOffsetColumns": True,
                             "offsetColumns": [offset_col]}]

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

        primary_key_specification_expected_json = json.dumps(
            json.loads(PRIMARY_KEY_NON_NUMERIC_METADATA_ORACLE),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        if connection is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f'drop table {table_name}')
            connection.close()


def test_oracle_vendor_header(sdc_builder, sdc_executor, database):
    """Validate that the primary key (and no other columns) information is present in the record headers. """
    table_name = get_random_string(string.ascii_uppercase, 10)
    connection = None
    vendor_specification = f"jdbc.vendor"

    INPUT_DATA = [
        {'id': 1, 'name': 'The Lich King', 'game': 'World of Warcraft'},
        {'id': 2, 'name': 'Bowser', 'game': 'Super Mario Bros'},
        {'id': 3, 'name': 'Handsome Jack', 'game': 'Borderlands 2'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('Oracle Multitable Consumer')
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

            vendor_metadata_expected = 'Oracle'

            assert vendor_metadata == vendor_metadata_expected

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
        if connection is not None:
            connection.close()


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


def test_oracle_multitable_oracle_no_leaking_cursors(sdc_builder, sdc_executor, database):
    """By default the maximum number of open cursors in Oracle is 300. By making the multitable stage
    be reading 1000 tables we are forcing the cursors leakage detected in ESC-2017 appears and makes
    the pipeline startup fails (StartError).
    """

    connection = database.engine.connect()
    schema = database.username.upper()
    table_name = "test_no_leaking_cursors_".upper()
    try:
        connection.execute(f"""
        BEGIN
        FOR v_tableCounter IN 1..1000 LOOP
            EXECUTE IMMEDIATE 'create table {schema}.{table_name}' || v_tableCounter || '(id NUMBER, text varchar(10) DEFAULT NULL)';
            FOR v_loopCounter IN 1..10 LOOP
                EXECUTE IMMEDIATE 'INSERT INTO {schema}.{table_name}'|| v_tableCounter || '(id) VALUES (1)';
            END LOOP;
        END LOOP
        COMMIT;
        END;
        """)

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Oracle Multitable Consumer')
        origin.set_attributes(
            maximum_number_of_tables=-1,
            table_configs=[{
                "tablePattern": f'{table_name}%',
                "enableNonIncremental": True,
            }],
            per_batch_strategy='PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE'
        )
        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1_000)  # No need to read them all
        sdc_executor.stop_pipeline(pipeline)
    except StartError as e:
        assert False, f'Pipeline failed to start, when reaching the max open cursors it used to fail with a JDBC_640: {e}'
    finally:
        connection.execute(f"""
        BEGIN
            FOR v_tableCounter IN 1..1000 LOOP
                EXECUTE IMMEDIATE 'drop table {schema}.test_no_leaking_cursors_' || v_tableCounter;
            END LOOP
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        """)
        if connection is not None:
            connection.close()
            
