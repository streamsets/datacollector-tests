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
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.connx, sdc_min_version('5.4.0')]

TABLE_NAME = get_random_string(string.ascii_letters, 15)
TRANSFORM_NAME = get_random_string(string.ascii_letters, 15)
TABLE_ID = random.randint(100,100000)

@pytest.fixture(scope="module", autouse=True)
def _set_up_CDC(connx):
    try:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                logger.info(f'Create a table and enable CDC on it')
                cursor.execute(f'CREATE TABLE localhost.dbo.{TABLE_NAME} (ID INTEGER, Name varchar(255) NOT NULL, PRIMARY KEY (ID));')
                cursor.execute(f'CREATE UNIQUE INDEX {TABLE_NAME}_INDEX ON localhost.dbo.{TABLE_NAME} (ID);')
                cursor.execute(
                    f'INSERT INTO CONNXDataSync.datasync.TableList (TableID,TableName,SynchronizationCategoryID,'
                    f'TransformSQL,TransformTargetTable,TransformSourceTableForGuiPath,TimeStampFilterField,'
                    f'LastDataTimestamp,DaylightSavingsType,DaylightSavingsBegins,DaylightSavingsEnds,DriftSeconds,'
                    f'DropAndRecreate,PurgeUsingDelete) VALUES ({TABLE_ID},\'{TRANSFORM_NAME}\',1,\'SELECT * FROM "localhost"."dbo"."{TABLE_NAME}"\','
                    f'\'\',\'localhost.dbo.{TABLE_NAME}\',\'\',NULL,\'\',NULL,NULL,5,1,1);')
                cursor.execute(
                    f'INSERT INTO CONNXDataSync.datasync.TableMapper (TableID,TargetColumnOrdinal,SourceExpression,'
                    f'SourceDataType,TargetColumn,TargetDataType,TargetSqlDataType,TargetSqlLength,TargetScale,'
                    f'TargetPrecision,TargetIsNullable,SourceColumnOrdinal)VALUES ({TABLE_ID},0,\'ID\',\'4\',\'ID\',\'\','
                    f'4,4,0,0,1,0);')
                cursor.execute(
                    f'INSERT INTO CONNXDataSync.datasync.TableMapper (TableID,TargetColumnOrdinal,SourceExpression,'
                    f'SourceDataType,TargetColumn,TargetDataType,TargetSqlDataType,TargetSqlLength,TargetScale,'
                    f'TargetPrecision,TargetIsNullable,SourceColumnOrdinal)VALUES ({TABLE_ID},1,\'Name\',\'-8\',\'Name\',\'\','
                    f'-8,35,0,0,0,1);')
                cursor.execute(
                    f'INSERT INTO CONNXDataSync.datasync.TargetTableIndex (TableID,IndexID,IsUnique,IsPrimary,IsIndexUsedForSync,IndexName) '
                    f'VALUES ({TABLE_ID},1,-1,-1,NULL,\'{TABLE_NAME}_INDEX\');')
                cursor.execute(
                    f'INSERT INTO CONNXDataSync.datasync.TargetTableIndexColumns (TableID,IndexID,ColumnSequence,ColumnName,'
                    f'ColumnSortOrder) VALUES ({TABLE_ID},1,1,\'ID\',\'ASC\');')
                cursor.execute(f'SELECT create_crc_baseline(\'{TRANSFORM_NAME}\');')
                cursor.execute(f'SELECT create_crc_savepoint(\'{TRANSFORM_NAME}\');')
                cursor.execute(f'SELECT create_crc_finalize(\'{TRANSFORM_NAME}\');')

        yield

    finally:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                logger.info(f'Tear the environment down')
                cursor.execute(f'delete from CONNXDataSync.datasync.TableList where TableName = \'{TRANSFORM_NAME}\';')
                cursor.execute(f'delete from CONNXDataSync.datasync.TableMapper where TableID = {TABLE_ID};')
                cursor.execute(f'delete from CONNXDataSync.datasync.TargetTableIndex where TableID = {TABLE_ID};')
                cursor.execute(f'delete from CONNXDataSync.datasync.TargetTableIndexColumns where TableID = {TABLE_ID};')
                cursor.execute(f'drop table localhost.dbo.{TABLE_NAME};')


def test_connx_cdc_insert(sdc_builder, sdc_executor, connx):
    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX CDC')
    origin.set_attributes(datasync_transform=f'{TRANSFORM_NAME}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                logger.info(f'Creating some CDC data')
                cursor.execute(f'select create_crc_finalize(\'{TRANSFORM_NAME}\');')
                cursor.execute(f"insert into localhost.dbo.{TABLE_NAME} values (1, 'Name');")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].field['Name'] == 'Name'
    finally:
        logger.info('Removing created data')
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"delete from localhost.dbo.{TABLE_NAME} where Name = 'Name';")


def test_connx_cdc_update(sdc_builder, sdc_executor, connx):
    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX CDC')
    origin.set_attributes(datasync_transform=f'{TRANSFORM_NAME}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                logger.info(f'Creating some CDC data')
                cursor.execute(f"insert into localhost.dbo.{TABLE_NAME} values (1, 'Original');")
                cursor.execute(f'select create_crc_finalize(\'{TRANSFORM_NAME}\');')
                cursor.execute(f"update localhost.dbo.{TABLE_NAME} set Name = 'Changed' where Name = 'Original';")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['ID'] == 1
        assert wiretap.output_records[0].field['Name'] == 'Changed'
    finally:
        logger.info('Removing created data')
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"delete from localhost.dbo.{TABLE_NAME} where Name = 'Changed';")


def test_connx_cdc_delete(sdc_builder, sdc_executor, connx):
    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX CDC')
    origin.set_attributes(datasync_transform=f'{TRANSFORM_NAME}',
                          use_credentials=True)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(connx)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Finalize and create CDC data
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                logger.info(f'Creating some CDC data')
                cursor.execute(f"insert into localhost.dbo.{TABLE_NAME} values (1, 'Name');")
                cursor.execute(f'select create_crc_finalize(\'{TRANSFORM_NAME}\');')
                cursor.execute(f"delete from localhost.dbo.{TABLE_NAME} where Name = 'Name';")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1
        wiretap.output_records[0].field['ID'] == 1
    finally:
        logger.info('Removing created data (in case the pipeline failed)')
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"delete from localhost.dbo.{TABLE_NAME} where Name = 'Name';")
