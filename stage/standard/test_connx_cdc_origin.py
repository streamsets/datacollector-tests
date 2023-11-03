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

import datetime
import time
import logging
import string
import random

import pytest
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.connx, sdc_min_version('5.4.0')]

def prepare_CDC(cursor, table_name, table_id, transform_name):
    logger.info(f'Creating table {table_name}...')
    cursor.execute(
        f'CREATE TABLE localhost.dbo.{table_name} (id INTEGER, tinyintval TINYINT, smallintval SMALLINT,'
        f'intval INT, decimalval DECIMAL(5,2), numericval NUMERIC(5,2), doubleval DOUBLE,'
        f'realval REAL, bitval BIT, dateval DATE, timestampval TIMESTAMP, charval CHAR(5),'
        f'varcharval VARCHAR(5), longvarcharval LONGVARCHAR(5), ncharval NCHAR(5), binaryval BINARY,'
        f'varbinaryval VARBINARY(8), longvarbinaryval LONGVARBINARY(8), PRIMARY KEY (ID));')
    logger.info(f'Creating table index for {table_name}...')
    cursor.execute(f'CREATE UNIQUE INDEX {table_name}_INDEX ON localhost.dbo.{table_name} (ID);')
    logger.info(f'Adding table {table_name} to DataSync TableList...')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TableList (TableID,TableName,SynchronizationCategoryID,'
        f'TransformSQL,TransformTargetTable,TransformSourceTableForGuiPath,TimeStampFilterField,'
        f'LastDataTimestamp,DaylightSavingsType,DaylightSavingsBegins,DaylightSavingsEnds,DriftSeconds,'
        f'DropAndRecreate,PurgeUsingDelete) VALUES ({table_id},\'{transform_name}\',1,\'SELECT * FROM "localhost"."dbo"."{table_name}"\','
        f'\'\',\'localhost.dbo.{table_name}\',\'\',NULL,\'\',NULL,NULL,5,1,1);')
    logger.info(f'Adding table {table_name} to DataSync TableMapper...')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TableMapper (TableID,TargetColumnOrdinal,SourceExpression,'
        f'SourceDataType,TargetColumn,TargetDataType,TargetSqlDataType,TargetSqlLength,TargetScale,'
        f'TargetPrecision,TargetIsNullable,SourceColumnOrdinal)VALUES ({table_id},0,\'ID\',\'4\',\'ID\',\'\','
        f'4,4,0,0,1,0);')
    logger.info(f'Adding table {table_name} to DataSync TargetTableIndex...')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TargetTableIndex (TableID,IndexID,IsUnique,IsPrimary,IsIndexUsedForSync,IndexName) '
        f'VALUES ({table_id},1,-1,-1,NULL,\'{table_name}_INDEX\');')
    logger.info(f'Adding table {table_name} to DataSync TargetTableIndexColumns...')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TargetTableIndexColumns (TableID,IndexID,ColumnSequence,ColumnName,'
        f'ColumnSortOrder) VALUES ({table_id},1,1,\'ID\',\'ASC\');')
    logger.info(f'Executing CRC operations for transform {transform_name}...')
    cursor.execute(f'SELECT create_crc_baseline(\'{transform_name}\');')
    cursor.execute(f'SELECT create_crc_savepoint(\'{transform_name}\');')
    cursor.execute(f'SELECT create_crc_finalize(\'{transform_name}\');')

def tear_down_CDC(cursor, table_name, table_id):
    logger.info(f'Tear the environment down')
    try:
        cursor.execute(f'delete from CONNXDataSync.datasync.TableList where TableID = {table_id};')
        cursor.execute(f'delete from CONNXDataSync.datasync.TableMapper where TableID = {table_id};')
        cursor.execute(f'delete from CONNXDataSync.datasync.TargetTableIndex where TableID = {table_id};')
        cursor.execute(f'delete from CONNXDataSync.datasync.TargetTableIndexColumns where TableID = {table_id};')
        cursor.execute(f'drop table localhost.dbo.{table_name};')
    except:
        logger.info(f'Table {table_name} already deleted')
        pass

# https://www.connx.com/products/connx/CONNX%2013.5%20UserGuide/default.htm#connxcdd32c/ole_db_data_types.htm
# Omitting BIGINT and TIME for the time being since CONNX when used in the underlying MySQL table CONNX maps both of
# these types to VARCHAR instead, as according to the documentation CONNX decides the mapping of types to the SQL Type it
# deems most appropiate.
DATA_TYPES_CONNX = [
    ('tinyintval', '-128', 'SHORT', -128),
    ('smallintval', '-32768', 'SHORT', -32768),
    ('intval', '-2147483648', 'INTEGER', '-2147483648'),
    #('BIGINT', '9223372036854775807', 'LONG', '9223372036854775807'),
    ('decimalval', '5.20', 'DECIMAL', '5.20'),
    ('numericval', '5.20', 'DECIMAL', '5.20'),
    ('doubleval', '5.2', 'DOUBLE', '5.2'),
    ('realval', '5.2', 'FLOAT', '5.2'),
    ('bitval','0', 'BOOLEAN', False),
    ('dateval', "'2019-01-01'", 'DATE', datetime.datetime(2019, 1, 1)),
    ('timestampval', "'2019-01-01 5:00:00'", 'DATETIME', datetime.datetime(2019, 1, 1, 5, 0, 0)),
    #('TIME', "'5:00:00'", 'TIME', 18000000),
    ('charval', "'Hello'", 'STRING', 'Hello'),
    ('varcharval', "'Hello'", 'STRING', 'Hello'),
    ('longvarcharval', "'Hello'", 'STRING', 'Hello'),
    ('ncharval', "'Hello'", 'STRING', 'Hello'),
    ('binaryval', '1', 'BYTE_ARRAY', 'MQ=='),
    ('varbinaryval', "'10101010'", 'BYTE_ARRAY', 'MTAxMDEwMTA='),
    ('longvarbinaryval', "'Hello'", 'BYTE_ARRAY', 'DgA='),
]


@pytest.mark.parametrize('connx_type,insert_fragment,expected_type,expected_value', DATA_TYPES_CONNX, ids=[i[0] for i in DATA_TYPES_CONNX])
def test_data_types(sdc_builder, sdc_executor, connx_type, connx, insert_fragment, expected_type, expected_value):
    """Test all feasible CONNX types."""
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    try:
        prepare_CDC(cursor, table_name, table_id, transform_name)

        logger.info('Inserting data into %s', table_name)
        cursor.execute(f"INSERT INTO {table_name} (id, {connx_type}) VALUES(1,{insert_fragment})")
        cursor.execute(f"INSERT INTO {table_name} (id, {connx_type}) VALUES(2,NULL)")

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('CONNX CDC')
        origin.datasync_transform = transform_name

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

        assert record.field[connx_type].type == expected_type
        assert null_record.field[connx_type].type == expected_type

        if (expected_type == 'DATE' or expected_type == 'DATETIME'):
            # Convert expected date to UNIX timestamp and convert to milliseconds
            expected_value = time.mktime(expected_value.timetuple())*1000

        assert record.field[connx_type]._data['value'] == expected_value
        assert null_record.field[connx_type] == None
    finally:
        tear_down_CDC(cursor, table_name, table_id)


def test_object_names(sdc_builder, sdc_executor, connx):
    pytest.skip("The CONNX CDC origin doesn't generate queries - it only takes user input, thus user is responsible to"
                "properly escape or enclose names and thefore there is not much for us to test here.")


def test_multiple_batches(sdc_builder, sdc_executor, connx):
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    max_batch_size = 20
    batches = 10

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('CONNX CDC')
    origin.datasync_transform = transform_name
    origin.max_batch_size_in_records = max_batch_size

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    origin >= finisher

    pipeline = builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        prepare_CDC(cursor, table_name, table_id, transform_name)

        logger.info('Inserting data into %s', table_name)
        for n in range(1,  max_batch_size * batches + 1):
            cursor.execute(f"INSERT INTO {table_name} (id) VALUES({n})")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(max_batch_size * batches)

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
        tear_down_CDC(cursor, table_name, table_id)


def test_dataflow_events(sdc_builder, sdc_executor, connx):
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('CONNX CDC')
    origin.datasync_transform = transform_name

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'connx-finalize-success'}"])

    trash = builder.add_stage("Trash")
    origin >> trash

    wiretap = builder.add_wiretap()
    origin >= [wiretap.destination, finisher]

    pipeline = builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        prepare_CDC(cursor, table_name, table_id, transform_name)

        logger.info('Inserting data into %s', table_name)
        for n in range(1, 101):
            cursor.execute(f"INSERT INTO {table_name} (id) VALUES({n})")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        # We can't guarantee that the pipeline stops after doing only one iteration of produce, so we cannot assume that
        # only the 5 events that we expect have been generated.
        assert len(records) == 5

        # First event is always a CRC savepoint
        assert records[0].header.values['sdc.event.type'] == 'connx-savepoint-success'
        assert 'timestamp' in records[0].field
        assert 'query' in records[0].field

        # Second event is the SELECT from the INSERTS table
        assert records[1].header.values['sdc.event.type'] == 'connx-insert-success'
        assert records[1].field['rows'] == 100
        assert 'timestamp' in records[1].field
        assert 'query' in records[1].field

        # Third event is the SELECT from the UPDATES table
        assert records[2].header.values['sdc.event.type'] == 'connx-update-success'
        assert records[2].field['rows'] == 0
        assert 'timestamp' in records[2].field
        assert 'query' in records[2].field

        # Second event is the SELECT from the DELETES table
        assert records[3].header.values['sdc.event.type'] == 'connx-delete-success'
        assert records[3].field['rows'] == 0
        assert 'timestamp' in records[3].field
        assert 'query' in records[3].field

        # Final event is always a CRC finalize
        assert records[4].header.values['sdc.event.type'] == 'connx-finalize-success'
        assert 'timestamp' in records[4].field
        assert 'query' in records[4].field
    finally:
        tear_down_CDC(cursor, table_name, table_id)


def test_data_format(sdc_builder, sdc_executor, connx, keep_data):
    pytest.skip("CONNX Origin doesn't deal with data formats")


def test_resume_offset(sdc_builder, sdc_executor, connx, keep_data):
    pytest.skip("The CONNX CDC Origin needs to capture a new snapshot every time it tries to gather new data. Therefore, "
                "it does not use offsets as it needs to rely on CONNX tracking the data via keeping said snapshots up to date.")


