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
import random

import pytest
from streamsets.sdk.exceptions import StartError, StartingError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.connx, sdc_min_version('5.4.0')]


def prepare_CDC(cursor, table_name, table_id, transform_name):
    logger.info(f'Create a table and enable CDC on it')
    cursor.execute(f'CREATE TABLE localhost.dbo.{table_name} (ID INTEGER, Name varchar(255) NOT NULL, PRIMARY KEY (ID));')
    cursor.execute(f'CREATE UNIQUE INDEX {table_name}_INDEX ON localhost.dbo.{table_name} (ID);')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TableList (TableID,TableName,SynchronizationCategoryID,'
        f'TransformSQL,TransformTargetTable,TransformSourceTableForGuiPath,TimeStampFilterField,'
        f'LastDataTimestamp,DaylightSavingsType,DaylightSavingsBegins,DaylightSavingsEnds,DriftSeconds,'
        f'DropAndRecreate,PurgeUsingDelete) VALUES ({table_id},\'{transform_name}\',1,\'SELECT * FROM "localhost"."dbo"."{table_name}"\','
        f'\'\',\'localhost.dbo.{table_name}\',\'\',NULL,\'\',NULL,NULL,5,1,1);')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TableMapper (TableID,TargetColumnOrdinal,SourceExpression,'
        f'SourceDataType,TargetColumn,TargetDataType,TargetSqlDataType,TargetSqlLength,TargetScale,'
        f'TargetPrecision,TargetIsNullable,SourceColumnOrdinal)VALUES ({table_id},0,\'ID\',\'4\',\'ID\',\'\','
        f'4,4,0,0,1,0);')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TableMapper (TableID,TargetColumnOrdinal,SourceExpression,'
        f'SourceDataType,TargetColumn,TargetDataType,TargetSqlDataType,TargetSqlLength,TargetScale,'
        f'TargetPrecision,TargetIsNullable,SourceColumnOrdinal)VALUES ({table_id},1,\'Name\',\'-8\',\'Name\',\'\','
        f'-8,35,0,0,0,1);')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TargetTableIndex (TableID,IndexID,IsUnique,IsPrimary,IsIndexUsedForSync,IndexName) '
        f'VALUES ({table_id},1,-1,-1,NULL,\'{table_name}_INDEX\');')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.TargetTableIndexColumns (TableID,IndexID,ColumnSequence,ColumnName,'
        f'ColumnSortOrder) VALUES ({table_id},1,1,\'ID\',\'ASC\');')
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


def create_group(cursor, group_id, group_name):
    logger.info(f'Creating group {group_name} with id {group_id}')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.Groups (GroupID, GroupName, TargetDatabase, TargetPrefix, TargetExt, '
        f'GroupIsCdcChangesOnly) VALUES ({group_id}, \'{group_name}\', NULL, NULL, NULL, NULL);')


def insert_table_into_group(cursor, group_id, table_id):
    logger.info(f'Inserting table {table_id} into group {group_id}')
    cursor.execute(
        f'INSERT INTO CONNXDataSync.datasync.GroupMembers (GroupID, TableID) values ({group_id}, {table_id})')


def delete_group(cursor, group_id):
    logger.info(f'Deleting group with id {group_id}')
    cursor.execute(f'DELETE FROM CONNXDataSync.datasync.Groups WHERE GroupID = {group_id};')
    cursor.execute(f'DELETE FROM CONNXDataSync.datasync.GroupMembers WHERE GroupID = {group_id};')


def check_connx_version(cursor):
    logger.info(f'Checking if CONNX version supports groups')
    cursor.execute(
        f'select convert(substr(connx_version,1,2), integer) majorver, convert(substr(connx_version,4,2),integer) '
        f'as minorver, convert(substr(connx_version,7,5),integer) as buildnum')
    version = cursor.fetchall()
    if (version[0][0] <= 14 and version[0][1] <= 80 and version[0][2] <= 23301):
        pytest.skip(f"CONNX only supports groups from version 14.80.23301 onwards")

def test_connx_cdc_insert(sdc_builder, sdc_executor, connx):
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group=f'{transform_name}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        prepare_CDC(cursor, table_name, table_id, transform_name)
        logger.info(f'Creating some CDC data')
        cursor.execute(f'select create_crc_finalize(\'{transform_name}\');')
        cursor.execute(f"insert into localhost.dbo.{table_name} values (1, 'Name');")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].field['Name'] == 'Name'
    finally:
        tear_down_CDC(cursor, table_name, table_id)


def test_connx_cdc_update(sdc_builder, sdc_executor, connx):
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group=f'{transform_name}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        prepare_CDC(cursor, table_name, table_id, transform_name)
        logger.info(f'Creating some CDC data')
        cursor.execute(f"insert into localhost.dbo.{table_name} values (1, 'Original');")
        cursor.execute(f'select create_crc_finalize(\'{transform_name}\');')
        cursor.execute(f"update localhost.dbo.{table_name} set Name = 'Changed' where Name = 'Original';")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].field['Name'] == 'Changed'
    finally:
        tear_down_CDC(cursor, table_name, table_id)


def test_connx_cdc_delete(sdc_builder, sdc_executor, connx):
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group=f'{transform_name}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        prepare_CDC(cursor, table_name, table_id, transform_name)
        logger.info(f'Creating some CDC data')
        cursor.execute(f"insert into localhost.dbo.{table_name} values (1, 'Name');")
        cursor.execute(f"SELECT create_crc_savepoint('{transform_name}');")
        cursor.execute(f"select create_crc_finalize('{transform_name}');")
        cursor.execute(f"delete from localhost.dbo.{table_name} where Name = 'Name';")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1
        wiretap.output_records[0].field['ID'] == 1
    finally:
        tear_down_CDC(cursor, table_name, table_id)


def test_connx_cdc_operation_type(sdc_builder, sdc_executor, connx):
    table_name = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group=f'{transform_name}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        prepare_CDC(cursor, table_name, table_id, transform_name)
        logger.info(f'Creating some CDC data')
        # Create a new baseline for the test to clean up anything that happened previously
        cursor.execute(f'SELECT create_crc_savepoint(\'{transform_name}\');')
        cursor.execute(f'select create_crc_finalize(\'{transform_name}\');')
        # Add a couple of rows and turn that into the new baseline
        cursor.execute(f"insert into localhost.dbo.{table_name} values (3, 'Name');")
        cursor.execute(f"insert into localhost.dbo.{table_name} values (2, 'Name');")
        cursor.execute(f'SELECT create_crc_savepoint(\'{transform_name}\');')
        cursor.execute(f'select create_crc_finalize(\'{transform_name}\');')
        # Remove one of the existing rows, update the other and insert a third one
        cursor.execute(f"delete from localhost.dbo.{table_name} where ID = 3;")
        cursor.execute(f"update localhost.dbo.{table_name} set Name = 'Changed' where ID = 2;")
        cursor.execute(f"insert into localhost.dbo.{table_name} values (1, 'Name');")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3)
        sdc_executor.stop_pipeline(pipeline)

        # CONNX CDC always reads from the INSERTS table, then UPDATES, then DELETES, so regardless of the order that data
        # was inserted we will always see all operations in this order
        assert len(wiretap.output_records) == 3
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].header.values['sdc.operation.type'] == '4' # UPSERT code
        assert wiretap.output_records[1].field['ID'] == 2
        assert wiretap.output_records[1].header.values['sdc.operation.type'] == '3' # UPDATE code
        assert wiretap.output_records[2].field['ID'] == 3
        assert wiretap.output_records[2].header.values['sdc.operation.type'] == '2' # DELETE code
    finally:
        tear_down_CDC(cursor, table_name, table_id)


@sdc_min_version('5.9.0')
def test_connx_cdc_transform_group(sdc_builder, sdc_executor, connx):
    # If we use a group name in the CONNX CDC origin it will get the changes from all transforms in that group
    table_name = get_random_string(string.ascii_letters, 15)
    table_name_2 = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    transform_name_2 = get_random_string(string.ascii_letters, 15)
    group_name = get_random_string(string.ascii_letters, 15)
    table_id = random.randint(100, 100000)
    table_id_2 = random.randint(100, 100000)
    group_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()
    check_connx_version(cursor)

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group=f'{group_name}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        prepare_CDC(cursor, table_name, table_id, transform_name)
        prepare_CDC(cursor, table_name_2, table_id_2, transform_name_2)
        create_group(cursor, group_id, group_name)
        insert_table_into_group(cursor, group_id, table_id)
        insert_table_into_group(cursor, group_id, table_id_2)
        logger.info(f'Creating some CDC data')
        cursor.execute(f"select create_crc_finalize('{transform_name}');")
        cursor.execute(f"select create_crc_finalize('{transform_name_2}');")
        cursor.execute(f"insert into localhost.dbo.{table_name} values (1, 'Name');")
        cursor.execute(f"insert into localhost.dbo.{table_name_2} values (2, 'Name');")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].field['Name'] == 'Name'
        assert wiretap.output_records[1].field['ID'] == 2
        assert wiretap.output_records[1].field['Name'] == 'Name'
    finally:
        tear_down_CDC(cursor, table_name, table_id)
        delete_group(cursor, group_id)


@sdc_min_version('5.9.0')
def test_connx_cdc_transform_group_same_name(sdc_builder, sdc_executor, connx):
    # If there is a transform and a group with the same name we should only get the changes from the transform
    table_name = get_random_string(string.ascii_letters, 15)
    table_name_2 = get_random_string(string.ascii_letters, 15)
    transform_name = get_random_string(string.ascii_letters, 15)
    transform_name_2 = get_random_string(string.ascii_letters, 15)
    group_name = transform_name
    table_id = random.randint(100, 100000)
    table_id_2 = random.randint(100, 100000)
    group_id = random.randint(100, 100000)
    cursor = connx.get_connection().cursor()
    check_connx_version(cursor)

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group=f'{group_name}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        prepare_CDC(cursor, table_name, table_id, transform_name)
        prepare_CDC(cursor, table_name_2, table_id_2, transform_name_2)
        create_group(cursor, group_id, group_name)
        insert_table_into_group(cursor, group_id, table_id)
        insert_table_into_group(cursor, group_id, table_id_2)
        logger.info(f'Creating some CDC data')
        cursor.execute(f"select create_crc_finalize('{transform_name}');")
        cursor.execute(f"select create_crc_finalize('{transform_name_2}');")
        cursor.execute(f"insert into localhost.dbo.{table_name} values (1, 'Name');")
        cursor.execute(f"insert into localhost.dbo.{table_name_2} values (2, 'Name');")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Since the group name is the same as the transform name for table 1, we will only get results from that table
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].field['Name'] == 'Name'
    finally:
        tear_down_CDC(cursor, table_name, table_id)


@sdc_min_version('5.9.0')
def test_connx_cdc_invalid_transform_group(sdc_builder, sdc_executor, connx):
    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('CONNX CDC')
    origin.set_attributes(datasync_transform_or_group='INVALID_TRANSFORM_OR_GROUP_NAME',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        pytest.fail('This point should not be reached')
    except (StartError, StartingError) as error:
        assert 'CONNX_01 - Specified transform or group name does not exist: INVALID_TRANSFORM_OR_GROUP_NAME' in error.message
