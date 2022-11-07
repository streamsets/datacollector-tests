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
import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.sdc_min_version('5.4.0'), pytest.mark.database('postgresql')]

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]


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


def test_postgres_lookup_processor(sdc_builder, sdc_executor, database, credential_store):
    """Simple PostgreSQL Lookup processor test.
    Pipeline will enrich records with the 'name' by adding a field as 'FirstName'.
    The pipeline looks like:
        dev_raw_data_source >> postgres_lookup >> wiretap
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database, quote=True)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(LOOKUP_RAW_DATA),
                                       stop_after_first_batch=True)

    postgres_lookup = pipeline_builder.add_stage('PostgreSQL Lookup')
    query_str = f'SELECT "name" FROM "{table_name}" WHERE "id" = ${{record:value("/id")}}'
    column_mappings = [dict(dataType='USE_COLUMN_TYPE',
                            columnName='name',
                            field='/FirstName')]
    postgres_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings,
                               ssl_mode='DISABLED')

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> postgres_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='PostgreSQL Lookup').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(ROWS_IN_DATABASE)
    for record in LOOKUP_EXPECTED_DATA:
        record.pop('id')
        record['FirstName'] = record.pop('name')

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows_from_wiretap = [{list(record.field.keys())[1]: list(record.field.values())[1].value}
                             for record in wiretap.output_records]
        assert rows_from_wiretap == LOOKUP_EXPECTED_DATA
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-16138: PostgreSQL Lookup Processor is not properly catching UncheckedExecutionException
def test_postgres_lookup_processor_incorrect_query_for_data(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_uppercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('key', sqlalchemy.Integer, primary_key=False, quote=True),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection = database.engine.connect()
        connection.execute(table.insert(), [
            {"key": 1, "id": 1},
            {"key": 2, "id": 1},
            {"key": 2, "id": 2}
        ])

        builder = sdc_builder.get_pipeline_builder()
        source = builder.add_stage('Dev Raw Data Source')
        source.data_format = 'JSON'
        source.raw_data = '{"key": 1 }\n{"key": 2}\n{"key": 1}\n'
        source.stop_after_first_batch = True

        lookup = builder.add_stage('PostgreSQL Lookup')
        # Query is intentionally correlated to create a SQL error (the correlated subsequery will return two rows
        # for the key=2 which makes the query invalid).
        lookup.sql_query = f"SELECT \"id\" FROM \"{table_name}\" WHERE \"id\" = (select \"id\" FROM \"{table_name}\" WHERE \"key\" = ${{record:value('/key')}})"
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='id', field='/id')]
        lookup.ssl_mode = 'DISABLED'

        wiretap = builder.add_wiretap()
        source >> lookup >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We are expecting 2 records that will lookup the id 1
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field['id'] == 1
        assert wiretap.output_records[1].field['id'] == 1
        # And one error record for the key 2
        assert len(wiretap.error_records) == 1
        assert wiretap.error_records[0].field['key'] == 2
        assert wiretap.error_records[0].header['errorCode'] == 'JDBC_02'
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            table.drop(database.engine)
