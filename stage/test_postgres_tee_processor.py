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

import copy
import json
import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY = [
    {'TYPE': 'Hobbit', 'ID': 1, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 0'},
    {'TYPE': 'Fallohide', 'ID': 1, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 1'},
    {'TYPE': 'Fallohide', 'ID': 2, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 2'},
    {'TYPE': 'Hobbit - Fallohide', 'ID': 3, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 3'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 3, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 4'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 4, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 5'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 4, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 6'}
]
CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_EXPECTED_DATA = [
    ('Hobbit, Fallohide', 4, 'Bilbo', 'Baggins', 'Bag End 6')
]
CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER = [
    {'sdc.operation.type': 1, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 2,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 2, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit - Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit - Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 4, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},
]


def _create_table(table_name, database, schema_name=None, quote=False):
    """Helper function to create a table with two columns: id (int, PK) and name (str).

    Args:
        table_name: (:obj:`str`) the name for the new table.
        database: a :obj:`streamsets.testframework.environment.Database` object.
        schema_name: (:obj:`str`, optional) when provided, create the new table in a specific schema; otherwise,
            the default schema for the engine’s database connection is used.

    Return:
        The new table as a sqlalchemy.Table object.

    """
    metadata = sqlalchemy.MetaData()

    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('name', sqlalchemy.String(32), quote=quote),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=quote),
                             schema=schema_name,
                             quote=quote)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


@database('postgresql')
@sdc_min_version('5.3.0')
def test_postgres_tee_processor(sdc_builder, sdc_executor, database):
    """Simple PostgreSQL Tee processor test.
    Pipeline will insert records into database and then pass generated database column 'id' to fields.
    The pipeline looks like:
        dev_raw_data_source >> postgres_tee >> wiretap
    """

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(RAW_DATA),
                                       stop_after_first_batch=True)

    postgres_tee = pipeline_builder.add_stage('PostgreSQL Tee')
    # Note that here ids are not inserted. Database generates them automatically.
    field_to_column_mapping = [dict(columnName='name',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/name',
                                    paramValue='?')]
    generated_column_mappings = [dict(columnName='id',
                                      dataType='USE_COLUMN_TYPE',
                                      field='/id')]
    postgres_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=field_to_column_mapping,
                            generated_column_mappings=generated_column_mappings,
                            table_name=table_name,
                            ssl_mode='DISABLED')

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> postgres_tee >> wiretap.destination
    pipeline = pipeline_builder.build(title='PostgreSQL Tee').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify the PostgreSQL Tee processor has got new ids which were generated by database.
        rows_from_wiretap = [{list(item.field.keys())[0]: list(item.field.values())[0].value,
                              list(item.field.keys())[1]: int(list(item.field.values())[1].value)}
                             for item in wiretap.output_records]
        assert rows_from_wiretap == ROWS_IN_DATABASE
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('postgresql')
@sdc_min_version('5.3.0')
@pytest.mark.parametrize('use_multi_row', [True, False])
def test_postgres_tee_processor_multi_ops(sdc_builder, sdc_executor, database, use_multi_row):
    """PostgreSQL Tee processor with multiple operations
    Pipeline will delete/update/insert records into database with one batch and then update 'id'
    field if it is inserted. The 'operation' field is used for the record header sdc.operation.type
    which defines the CRUD operation (1: Insert, 2: Delete, 3: Update). The pipeline looks like:
        dev_raw_data_source >> expression evaluator >> postgres_tee >> wiretap
    """

    table_name = get_random_string(string.ascii_lowercase, 20)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Jarcec', 'id': 2},  # delete
        {'operation': 3, 'name': 'Hari', 'id': 3},  # update
        {'operation': 1, 'name': 'Eddie'},  # insert, id will be added by PostgreSQL Tee
        {'operation': 1, 'name': 'Fran'}  # insert, id will be added by PostgreSQL Tee
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
    postgres_tee = pipeline_builder.add_stage('PostgreSQL Tee')
    postgres_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=FIELD_TO_COLUMN,
                            generated_column_mappings=[dict(columnName='id', field='/id')],
                            table_name=table_name,
                            use_multi_row_operation=use_multi_row,
                            ssl_mode='DISABLED')

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> expression_evaluator >> postgres_tee >> wiretap.destination
    pipeline_title = 'PostgreSQL Tee MultiOps MultiRow' if use_multi_row else 'PostgreSQL Tee MultiOps SingleRow'
    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table(table_name, database)
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'name': row['name']} for row in ROWS_IN_DATABASE])

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sequence_id = len(ROWS_IN_DATABASE)
        # Verify the database is updated.
        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        expected_data = [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
        for record in DATA:
            if record['operation'] == 1:  # insert
                sequence_id += 1
                expected_data.append((record['name'], sequence_id))
            elif record['operation'] == 2:  # delete
                expected_data = [row for row in expected_data if row[1] != record['id']]
            elif record['operation'] == 3:  # update
                expected_data = [row if row[1] != record['id'] else (record['name'], row[1]) for row in expected_data]
        assert data_from_database == expected_data

        # Verify the PostgreSQL Tee processor has the new ID which were generated by database.
        name_id_from_output = [(record.field['name'], record.field['id']) for record in wiretap.output_records]
        assert name_id_from_output == [('Jarcec', 2), ('Hari', 3), ('Eddie', 4), ('Fran', 5)]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('postgresql')
@sdc_min_version('5.3.0')
@pytest.mark.parametrize('multi_row', [False, True])
def test_postgres_tee_processor_primary_key_header_update(sdc_builder, sdc_executor, database, multi_row):
    """
    Test to make sure Primary Key updates are handled correctly. We will insert one record, and then
    perform multiple updates to its columns (including Primary Key columns) and check we end up
    having just one row in the table, with the expected values.
    The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_remover >> postgres_tee >> wiretap
    """

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    rows = copy.deepcopy(CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY)
    for row, header in zip(rows, CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER):
        row['HEADER'] = header
    raw_data = '\n'.join((json.dumps(row) for row in rows))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # We assume CDC headers are generated properly
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/HEADER/sdc.operation.type')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".ID')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".ID')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".TYPE')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".TYPE')}"}
    ])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/HEADER']

    # PostgreSQL Producer
    postgres_tee = pipeline_builder.add_stage('PostgreSQL Tee')
    # Note that here ids are not inserted. Database generates them automatically.
    postgres_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=[],
                            generated_column_mappings=[],
                            use_multi_row_operation=multi_row,
                            table_name=table_name,
                            enclose_table_name=True,
                            ssl_mode='DISABLED')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> field_remover >> postgres_tee >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('TYPE', sqlalchemy.String(64), primary_key=True),
                             sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('NAME', sqlalchemy.String(64)),
                             sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                             sqlalchemy.Column('ADDRESS', sqlalchemy.String(64)))
    try:
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()

        # We assert that the pipeline processed 1 insert and 6 updates (the extra output is due to wiretap)
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 7
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 7 + 1

        # And assert that in the database we end up having just 1 row, with the latest update value
        assert len(data_from_database) == 1
        assert data_from_database == CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_EXPECTED_DATA

        # And we will also check that the processor generated records with the corresponding updated values
        assert len(wiretap.output_records) == 7
        for record, expected_data in zip(wiretap.output_records, CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY):
            assert len(record.field) == len(expected_data.keys())  # make sure we have the number of fields needed
            for field in record.field:
                assert record.field[field] == expected_data[field]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)
