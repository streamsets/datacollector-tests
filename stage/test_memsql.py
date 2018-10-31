# Copyright 2018 StreamSets Inc.
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

import json
import logging
import string

import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Serena Williams'},
    {'id': 2, 'name': 'Simona Halep'},
    {'id': 3, 'name': 'Naomi Osaka'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie Park'}, # duplicated key
    {'id': 4, 'name': 'Kirti Velankar'}
]
RAW_DATA = ['id,name'] + [','.join(str(value) for value in row.values()) for row in ROWS_IN_DATABASE]


def _create_table_in_database(table_name, database):
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


@database('memsql', 'mysql')
@sdc_min_version('3.6.0')
def test_basic(sdc_builder, sdc_executor, database):
    """Test for MemSQL Fast Loader target stage. Data is inserted into MemSQL in the pipeline.
    Data is read from MemSQL using mysql client. We assert the data from the client to what has
    been ingested by the MemSQL pipeline.

    The pipeline looks like:
    MemSQL pipeline:
        dev_raw_data_source  >> MemSQL
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    table = _create_table_in_database(table_name, database)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(RAW_DATA),
                                       stop_after_first_batch=True)

    memsql_fast_loader = pipeline_builder.add_stage('MemSQL Fast Loader')
    memsql_fast_loader.set_attributes(field_to_column_mapping=[],
                                      table_name=table_name)

    dev_raw_data_source >> memsql_fast_loader

    pipeline = pipeline_builder.build(title='MemSQL Fast Loader').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('memsql', 'mysql')
@sdc_min_version('3.6.0')
def test_memsql_fast_loader_ignore_duplicate(sdc_builder, sdc_executor, database):
    """Test records with duplicate keys. The existing records should be kept with IGNORE option.

    The pipeline looks like:
        dev_raw_data_source >> memsql_fast_loader
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE),
                                       stop_after_first_batch=True)

    memsql_fast_loader = pipeline_builder.add_stage('MemSQL Fast Loader')
    memsql_fast_loader.set_attributes(duplicate_key_error_handling='IGNORE',
                                      field_to_column_mapping=[],
                                      table_name=table_name)

    dev_raw_data_source >> memsql_fast_loader

    pipeline = pipeline_builder.build(title='MemSQL Ignore Duplicate').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    connection = database.engine.connect()
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = connection.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id

        updated_names = {record['id']: record['name'] for record in ROWS_TO_UPDATE}
        updated_names.update({record['id']: record['name'] for record in ROWS_IN_DATABASE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in data_from_database]
    finally:
        connection.close()
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('memsql', 'mysql')
@sdc_min_version('3.6.0')
def test_memsql_fast_loader_replace_duplicate(sdc_builder, sdc_executor, database):
    """Test records with duplicate keys. The existing records should be replaced with REPLACE option.

    The pipeline looks like:
        dev_raw_data_source >> memsql_fast_loader
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE),
                                       stop_after_first_batch=True)

    memsql_fast_loader = pipeline_builder.add_stage('MemSQL Fast Loader')
    memsql_fast_loader.set_attributes(duplicate_key_error_handling='REPLACE',
                                      field_to_column_mapping=[],
                                      table_name=table_name)

    dev_raw_data_source >> memsql_fast_loader

    pipeline = pipeline_builder.build(title='MemSQL Replace Duplicate').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    connection = database.engine.connect()
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = connection.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id

        updated_names = {record['id']: record['name'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['id']: record['name'] for record in ROWS_TO_UPDATE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in data_from_database]
    finally:
        connection.close()
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('memsql', 'mysql')
@sdc_min_version('3.6.0')
def test_memsql_fast_loader_invalid_ops(sdc_builder, sdc_executor, database):
    """Test records with invalid operations. MemSQL Fast Loader does not support CDC origins which
    contains sdc.operation.type in the header. Expression Evaluator is used to add the header
    attributes. Records with any operation type should go to error records.

    The pipeline looks like:
        dev_raw_data_source >> expression evaluator >> memsql_fast_loader
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Simona Halep', 'id': 2}, # delete
        {'operation': 3, 'name': 'Eddie Park', 'id': 3}, # update
        {'operation': 1, 'name': 'Kirti Velankar', 'id': 4} # insert
    ]
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in DATA),
                                       stop_after_first_batch=True)

    HEADER_EXPRESSIONS = [dict(attributeToSet='sdc.operation.type',
                               headerAttributeExpression="${record:value('/operation')}")]
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = HEADER_EXPRESSIONS

    memsql_fast_loader = pipeline_builder.add_stage('MemSQL Fast Loader')
    memsql_fast_loader.set_attributes(duplicate_key_error_handling='REPLACE',
                                      field_to_column_mapping=[],
                                      table_name=table_name)

    dev_raw_data_source >> expression_evaluator >> memsql_fast_loader

    pipeline = pipeline_builder.build(title='MemSQL Invalid Ops').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    connection = database.engine.connect()
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        assert len(snapshot[memsql_fast_loader.instance_name].error_records) == len(DATA)

        result = connection.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        connection.close()
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)
