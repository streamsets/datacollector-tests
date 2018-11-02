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
import json
import logging
import random
import string

import pytest
import sqlalchemy
from streamsets.testframework.environments.databases import Oracle, SQLServerDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

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
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{src_table_prefix}%'}])

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
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{src_table_prefix}%'}])
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
    if type(database) == Oracle:
        pytest.skip('JDBC Tee Processor does not support Oracle')
    elif type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support multi row op on SQL Server')

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
@pytest.mark.parametrize('use_multi_row', [True, False])
@sdc_min_version('3.0.0.0') #stop_after_first_batch
def test_jdbc_tee_processor_multi_ops(sdc_builder, sdc_executor, database, use_multi_row):
    """JDBC Tee processor with multiple operations
    Pipeline will delete/update/insert records into database with one batch and then update 'id'
    field if it is inserted. The 'operation' field is used for the record header sdc.operation.type
    which defines the CRUD operation (1: Insert, 2: Delete, 3: Update). The pipeline looks like:
        dev_raw_data_source >> expression evaluator >> jdbc_tee >> trash
    """
    if type(database) == Oracle:
        pytest.skip('JDBC Tee Processor does not support Oracle')

    if use_multi_row == True and type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support multi row on SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Jarcec', 'id': 2}, # delete
        {'operation': 3, 'name': 'Hari', 'id': 3}, # update
        {'operation': 1, 'name': 'Eddie'} # insert, id will be added by JDBC Tee
    ]
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in DATA),
                                       stop_after_first_batch=True)

    HEADER_EXPRESSIONS = [dict(attributeToSet='sdc.operation.type',
                               headerAttributeExpression="${record:value('/operation')}")]
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = HEADER_EXPRESSIONS

    FIELD_TO_COLUMN = [dict(columnName='name', field='/name', paramValue='?')]
    jdbc_tee = pipeline_builder.add_stage('JDBC Tee')
    jdbc_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=FIELD_TO_COLUMN,
                            generated_column_mappings=[dict(columnName='id', field='/id')],
                            table_name=table_name,
                            use_multi_row_operation=use_multi_row)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> expression_evaluator >> jdbc_tee >> trash
    pipeline_title = 'JDBC Tee MultiOps MultiRow' if use_multi_row else 'JDBC Tee MultiOps SingleRow'
    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = create_table_in_database(table_name, database)
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection = database.engine.connect()
        # Passing only names to get the correct sequence numbers esp. PostgreSQL
        connection.execute(table.insert(), [{'name': row['name']} for row in ROWS_IN_DATABASE])

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sequence_id = len(ROWS_IN_DATABASE)
        # Verify the database is updated.
        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        expected_data = [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
        for record in DATA:
            if record['operation'] == 1: # insert
                sequence_id += 1
                expected_data.append((record['name'], sequence_id))
            elif record['operation'] == 2: # delete
                expected_data = [row for row in expected_data if row[1] != record['id']]
            elif record['operation'] == 3: # update
                expected_data = [row if row[1] != record['id'] else (record['name'], row[1]) for row in expected_data]
        assert data_from_database == expected_data

        # Verify the JDBC Tee processor has the new ID which were generated by database.
        jdbc_tee_output = snapshot[jdbc_tee].output
        name_id_from_output = [(record.field['name'], record.field['id']) for record in jdbc_tee_output]
        assert name_id_from_output == [('Jarcec', 2), ('Hari', 3), ('Eddie', sequence_id)]
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

    # SQL Server does not allow insert on identity column, 'id'
    if type(database) == SQLServerDatabase:
        query_str = f"INSERT INTO {table_name} (name) VALUES ('${{record:value('/name')}}')"

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
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def create_jdbc_producer_pipeline(pipeline_builder, pipeline_title, raw_data, table_name, operation):
    """Helper function to create and return a pipeline with JDBC Producer
    The Deduplicator assures there is only one ingest to database. The pipeline looks like:
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_producer
                               record_deduplicator >> trash
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    FIELD_MAPPINGS = [dict(field='/id', columnName='id'),
                      dict(field='/name', columnName='name')]
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation=operation,
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='STOP_PIPELINE')

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> jdbc_producer
    record_deduplicator >> trash

    return pipeline_builder.build(title=pipeline_title)


@database
def test_jdbc_producer_insert(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with INSERT operation.
    The pipeline inserts records into the database and verify that correct data is in the database.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = create_table_in_database(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Insert', DATA, table_name, 'INSERT')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_delete(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with DELETE operation.
    The pipeline deletes records from the database and verify that correct data is in the database.
    Records are deleted if the primary key is matched irrespective of other column values.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = create_table_in_database(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Delete', DATA, table_name, 'DELETE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        removed_ids = [record['id'] for record in ROWS_TO_UPDATE]
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE if record['id'] not in removed_ids]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_update(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with UPDATE operation.
    The pipeline updates records from the database and verify that correct data is in the database.
    Records with matching primary key are updated, and no action for unmatched records.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = create_table_in_database(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Update', DATA, table_name, 'UPDATE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        updated_names = {record['id']: record['name'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['id']: record['name'] for record in ROWS_TO_UPDATE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)
