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

import logging
import string
import pytest
import sqlalchemy

from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@database('postgresql')
def test_non_matching_types(sdc_builder, sdc_executor, database, keep_data):
    """Ensure proper error when a pre-existing table contains type mapping that is not valid."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # We don't support "money" in the Metadata processor
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                code money
            )
        """)

        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source')
        source.stop_after_first_batch = True
        source.data_format = 'JSON'
        source.raw_data = '{"id":1, "code": 2}'

        processor = builder.add_stage('PostgreSQL Metadata')
        processor.table_name = table_name

        wiretap = builder.add_wiretap()

        source >> processor >> wiretap.destination

        # Create & run the pipeline
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # The record should be sent to error with proper error code
        errors = wiretap.error_records
        assert len(errors) == 1
        assert errors[0].header['errorCode'] == 'JDBC_303'
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")


@database('postgresql')
def test_data_drift_between_batches(sdc_builder, sdc_executor, database):

    CSV01 = "fa,fb\na,b"
    CSV02 = "fa,fb,fc\na,b,c"  # add column fc
    schema_name = "schema_" + get_random_string(string.ascii_letters, 5).lower()
    table_name  = "table_" +  get_random_string(string.ascii_letters, 5).lower()
    temp_dir = sdc_executor.execute_shell(f'mktemp -d').stdout.rstrip()

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Directory')
    source.set_attributes(files_directory=temp_dir,
                          file_name_pattern="*",
                          data_format="DELIMITED",
                          header_line="WITH_HEADER")

    processor = builder.add_stage('PostgreSQL Metadata')
    processor.set_attributes(schema_name=schema_name,
                             table_name=table_name)

    destination = builder.add_stage('JDBC Producer')
    destination.set_attributes(schema_name=schema_name,
                               table_name=table_name,
                               default_operation="UPDATE",
                               field_to_column_mapping=[])

    source >> processor >> destination
    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    connection = database.engine.connect()
    try:
        # Create input files in SDC host
        sdc_executor.execute_shell(f'echo "{CSV01}" > {temp_dir}/01.csv && \
                                     echo "{CSV02}" > {temp_dir}/02.csv')

        # Create DB
        connection.execute(f"CREATE SCHEMA {schema_name}")
        connection.execute(f"CREATE TABLE {schema_name}.{table_name}(\
                                  fa varchar NULL,\
                                  fb varchar NULL,\
                                  CONSTRAINT {table_name}_pk PRIMARY KEY (fa)\
                                  );")
        connection.execute(f"INSERT INTO {schema_name}.{table_name} \
                                  (fa, fb) \
                                  VALUES('a', 'b');\
                                  ")

        # Run pipeline
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # Check results
        rs = connection.execute(f"SELECT fc FROM {schema_name}.{table_name} WHERE fa = 'a'")
        rows = [row for row in rs]
        assert len(rows) == 1, "Expected just 1 row"
        assert rows[0][0] == 'c', "Data drift value not present in database"

    finally:
        connection.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        connection.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        sdc_executor.execute_shell(f'rm -rf {temp_dir}')


@database('postgresql')
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('omit_constraints_when_creating_tables', [False, True])
@pytest.mark.parametrize('does_destination_table_exist', [False, True])
def test_omit_constraints_when_creating_tables(sdc_builder, sdc_executor, database, keep_data,
                                               omit_constraints_when_creating_tables, does_destination_table_exist):
    # Prepare data and table names
    num_records = 2
    input_data = [{'id': i,
                   'name': get_random_string(string.ascii_lowercase, 10),
                   'phone': get_random_string(string.ascii_lowercase, 10),
                   'email': get_random_string(string.ascii_lowercase, 10)}
                  for i in range(1, num_records + 1)]
    origin_table_name = f'origin_table_{get_random_string(string.ascii_lowercase, 20)}'
    destination_table_name = f'destination_table_{get_random_string(string.ascii_lowercase, 20)}'

    sql_query = f'SELECT * FROM {origin_table_name}' + ' WHERE id > ${OFFSET} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=num_records)

    processor = pipeline_builder.add_stage('PostgreSQL Metadata')
    processor.set_attributes(table_name=destination_table_name,
                             omit_constraints_when_creating_tables=omit_constraints_when_creating_tables)

    wiretap = pipeline_builder.add_wiretap()

    origin >> processor >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create and populate tables
        connection = database.engine.connect()
        logger.info('Creating origin table %s in %s database ...', origin_table_name, database.type)
        origin_table = sqlalchemy.Table(origin_table_name,
                                        sqlalchemy.MetaData(),
                                        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('name', sqlalchemy.String(32), primary_key=True),
                                        sqlalchemy.Column('phone', sqlalchemy.String(32), nullable=False),
                                        sqlalchemy.Column('email', sqlalchemy.String(32)))
        origin_table.create(database.engine)
        connection.execute(origin_table.insert(), input_data)
        if does_destination_table_exist:
            logger.info('Creating destination table %s in %s database ...', destination_table_name, database.type)
            destination_table = sqlalchemy.Table(destination_table_name,
                                                 sqlalchemy.MetaData(),
                                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                                 sqlalchemy.Column('name', sqlalchemy.String(32)),
                                                 sqlalchemy.Column('phone', sqlalchemy.String(32), nullable=False),
                                                 sqlalchemy.Column('email', sqlalchemy.String(32)))
            destination_table.create(database.engine)
            connection.execute(destination_table.insert(), input_data)

        # Run the pipeline
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records)
        sdc_executor.stop_pipeline(pipeline)

        # Inspecting the tables
        assert len(wiretap.output_records) == num_records,\
            "The output records does not correspond to the inputs read from the origin."

        # The destination table exists (has been created or already existed)
        inspector = sqlalchemy.inspect(database.engine)
        assert inspector.has_table(destination_table_name),\
            f"Destination table {destination_table_name} has not been created."

        # Primary Key and Nullable constraints check
        primary_keys = inspector.get_pk_constraint(destination_table_name)['constrained_columns']
        columns_nullable = []
        for column in inspector.get_columns(destination_table_name):
            # They keep the same order as they were created: id, name, phone, email
            columns_nullable.append(column['nullable'])

        assert len(columns_nullable) == 4,\
            f"There are {len(columns_nullable)} columns in the destination table when there should be 4."

        if omit_constraints_when_creating_tables and not does_destination_table_exist:
            # As desired, the table has been created with no constraints
            assert primary_keys == [] and \
                   columns_nullable == [True, True, True, True], \
                   "Table has been created with constraints!"
        elif not omit_constraints_when_creating_tables and not does_destination_table_exist:
            # The destination table has been created keeping the constraints of the origin table
            assert primary_keys == ['id', 'name'] and \
                   columns_nullable == [False, False, False, True], \
                   "Destination and origin table constraints do not match!"
        elif does_destination_table_exist:
            assert primary_keys == ['id'] and \
                   columns_nullable == [False, True, False, True], \
                   "Table constraints have changed!"

    finally:
        if not keep_data:
            for table in [origin_table_name, destination_table_name]:
                logger.info('Dropping table %s in %s database ...', table, database.type)
                connection.execute(f"DROP TABLE IF EXISTS \"{table}\"")
            if pipeline is not None:
                sdc_executor.remove_pipeline(pipeline)
