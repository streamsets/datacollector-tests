# Copyright 2017 StreamSets Inc.
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

import copy
import logging
import random
import string

import sqlalchemy

from testframework.markers import database
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]


@database
def test_jdbc_multitable_consumer_origin_simple(sdc_builder, sdc_executor, database):
    """
    Check if Jdbc Multi-table Origin can retrieve any records from a table.
    Destination is Trash.
    Verify input and output (via snapshot).
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configuration=[{"tablePattern": f'%{src_table_prefix}%'}])

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

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
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Column names are converted to lower case since Oracle database column names are in upper case.
        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/').lower():
                               record.value['value'][1]['value']}
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == [{'name': row['name']} for row in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_query_no_more_data(sdc_builder, sdc_executor, database):
    """
    This test case uses the JDBC Query origin.
    Test that Pipeline Finisher works.
    """
    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.configuration['isIncrementalMode'] = False
    jdbc_query_consumer.sql_query = 'SELECT * FROM {0}'.format(table_name)

    trash = pipeline_builder.add_stage('Trash')
    jdbc_query_consumer >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    jdbc_query_consumer >= finisher

    try:
        logger.info('Jdbc No More Data: Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    finally:
        logger.info('Jdbc No More Data: Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_multitable_consumer_with_finisher(sdc_builder, sdc_executor, database):
    """
    Test reading with Multi-table JDBC, output to trash.
    Test some table names that start with numbers (SDC-5381).
    Check if Pipeline Finished Executor works correctly.
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configuration=[{"tablePattern": f'%{src_table_prefix}%'}])
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >= finisher
    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    random.seed()

    tables = []
    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        num_letters = 10
        num_recs = 10
        num_tables = 3
        for i in range(0, num_tables):
            if i % 2 == 1:
                # table name starts with a number, contains mixed-case letters.
                input_name = '{}_{}_{}'.format(str(i), src_table_prefix,
                                               get_random_string(string.ascii_lowercase, num_letters))
            else:
                # table name comprised of mixed-case letters only.
                input_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, num_letters))

            tables.append(sqlalchemy.Table(
                input_name,
                metadata,
                sqlalchemy.Column('serial', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('data', sqlalchemy.Integer)
            ))
            tables[i].create(database.engine)

            rows = [{'serial': j, 'data': random.randint(0, 2100000000)} for j in range(1, num_recs + 1)]
            connection.execute(tables[i].insert(), rows)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    finally:
        for table in tables:
            table.drop(database.engine)


def create_table_in_database(table_name, database):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('name', sqlalchemy.String(32)),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    )
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


@database
def test_jdbc_lookup_processor(sdc_builder, sdc_executor, database):
    """Simple JDBC Lookup processor test.
    Pipeline will enrich records with the 'name' by adding a field as 'FirstName'.
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = create_table_in_database(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(LOOKUP_RAW_DATA))

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f"SELECT name FROM {table_name} WHERE id = '${{record:value('/id')}}'"
    column_mappings = [dict(dataType='USE_COLUMN_TYPE',
                            columnName='name',
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(ROWS_IN_DATABASE)
    for record in LOOKUP_EXPECTED_DATA:
        record.pop('id')
        record['FirstName'] = record.pop('name')

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/'): record.value['value'][1]['value']}
                              for record in snapshot[jdbc_lookup].output]
        assert rows_from_snapshot == LOOKUP_EXPECTED_DATA
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_tee_processor(sdc_builder, sdc_executor, database):
    """Simple JDBC Tee processor test.
    Pipeline will insert records into database and then pass generated database column 'id' to fields.
    The pipeline looks like:
        dev_raw_data_source >> jdbc_tee >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = create_table_in_database(table_name, database)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(RAW_DATA))

    jdbc_tee = pipeline_builder.add_stage('JDBC Tee')
    # Note that here ids are not inserted. Database generates them automatically.
    field_to_column_mapping = [dict(columnName='name',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/name',
                                    paramValue='?')]
    generated_column_mappings = [dict(columnName='id',
                                      dataType='USE_COLUMN_TYPE',
                                      field='/id')]
    jdbc_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=field_to_column_mapping,
                            generated_column_mappings=generated_column_mappings,
                            table_name=table_name)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_tee >> trash
    pipeline = pipeline_builder.build(title='JDBC Tee').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Verify the JDBC Tee processor has got new ids which were generated by database.
        rows_from_snapshot = [{item.value['value'][0]['sqpath'].strip('/'): item.value['value'][0]['value'],
                               item.value['value'][1]['sqpath'].strip('/'): int(item.value['value'][1]['value'])}
                              for item in snapshot[jdbc_tee].output]
        assert rows_from_snapshot == ROWS_IN_DATABASE
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_query_executor(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test.
    Pipeline will insert records into database and then using sqlalchemy, the verification will happen
    that correct data is inserted into database.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
                               record_deduplicator >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = create_table_in_database(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')
    query_str = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor.set_attributes(sql_query=query_str)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
    record_deduplicator >> trash
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(RAW_DATA) - 1)
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)
