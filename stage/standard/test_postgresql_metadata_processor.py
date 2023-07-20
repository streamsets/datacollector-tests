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
import json
import decimal
import datetime

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


DATA_TYPES = [
    ('true', 'BOOLEAN', True, 'boolean'),
    ('a', 'CHAR', 'a', 'character'),
#    ('a', 'BYTE', None, 'something'), # Not supported
    (120, 'SHORT', 120, 'smallint'),
    (120, 'INTEGER', 120, 'integer'),
    (120, 'LONG', 120, 'bigint'),
    (20.1, 'FLOAT', 20.1, 'real'),
    (20.1, 'DOUBLE', 20.1, 'double precision'),
    (20.1, 'DECIMAL', decimal.Decimal('20.10'), 'numeric'),
    ('2020-01-01 10:00:00', 'DATE', datetime.datetime(2020, 1, 1, 10, 0), 'date'),
    ('2020-01-01 10:00:00', 'TIME', datetime.datetime(2020, 1, 1, 10, 0), 'time without time zone'),
    ('2020-01-01 10:00:00', 'DATETIME', datetime.datetime(2020, 1, 1, 10, 0), 'timestamp without time zone'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00:00Z', 'timestamp with time zone'),
    ('string', 'STRING', 'string', 'character varying'),
    ('string', 'BYTE_ARRAY', b'string', 'bytea'),
]
@database('postgresql')
@pytest.mark.parametrize('input,converter_type,expected_value,expected_type', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, database, input, converter_type, expected_value, expected_type, keep_data):
    connection = database.engine.connect()
    table_name = get_random_string(string.ascii_letters, 10).lower()

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"value": input })

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/value'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_attribute_expressions = [{
        "fieldToSet": "/value",
        "attributeToSet": "precision",
        "fieldAttributeExpression": "5"
    },{
        "fieldToSet": "/value",
        "attributeToSet": "scale",
        "fieldAttributeExpression": "5"
    }]

    processor = builder.add_stage('PostgreSQL Metadata')
    processor.table_name = table_name

    wiretap = builder.add_wiretap()

    origin >> converter >> expression >> processor >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        # 1) Run the pipeline for the first time
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 1.1) We should create table in PostgreSQL
        rs = connection.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][0] == 'value'
        assert rows[0][1] == expected_type

        # 1.2) The pipeline should output one record that is unchanged
        output = wiretap.output_records
        assert len(output) == 1
        assert output[0].field['value'] == expected_value

        # Intermezzo - need to reset wiretap
        wiretap.reset()

        # 2) Let's run the pipeline again, this time the table already exists in the database
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 2.1) So the pipeline should just output the same (unchanged) record and be done
        output = wiretap.output_records
        assert len(output) == 1
        assert output[0].field['value'] == expected_value
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")


# Rules: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
# The processor was written to automatically lowercase all tables names, why? I have no clue, but can't easily
# change it without breaking backward compatibility.
OBJECT_NAMES = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_lowercase, 63), get_random_string(string.ascii_lowercase, 20)),
    ('max_column_name', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 63)),
    ('numbers', get_random_string(string.ascii_lowercase, 5) + "0123456789", get_random_string(string.ascii_lowercase, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_lowercase, 5) + "$_", get_random_string(string.ascii_lowercase, 5) + "$_"),
]
@database('postgresql')
@sdc_min_version('3.20.0')
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, test_name, table_name, column_name, database, keep_data):
    connection = database.engine.connect()
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 1 }}'
    source.stop_after_first_batch = True

    processor = builder.add_stage('PostgreSQL Metadata')
    processor.table_name = table_name

    trash = builder.add_stage('Trash')

    source >> processor >> trash
    pipeline = builder.build().configure_for_environment(database)

    try:
        sdc_executor.add_pipeline(pipeline)

        # Run the pipeline for the first time
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 1.1) We should create table in PostgreSQL
        rs = connection.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][0] == column_name
        assert rows[0][1] == 'integer'

        # Run the pipeline for the second time
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 2.1) No errors should be generated
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
    finally:
        if not keep_data:
                logger.info('Dropping table %s in %s database ...', table_name, database.type)
                connection.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")


@database('postgresql')
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    table_prefix = get_random_string(string.ascii_letters, 10).lower()
    connection = database.engine.connect()
    builder = sdc_builder.get_pipeline_builder()
    batch_size = 100
    batches = 100

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.delay_between_batches = 0
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    # We create 100 tables and iterate over them over and over again
    processor = builder.add_stage('PostgreSQL Metadata')
    processor.table_name = table_prefix + "_${record:value('/seq') % 100}"

    trash = builder.add_stage('Trash')

    origin >> processor >> trash
    pipeline = builder.build().configure_for_environment(database)

    try:
        sdc_executor.add_pipeline(pipeline)

        # Run the pipeline for the first time
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', batch_size * batches, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # 1.1) We should have 100 tables all with a single column
        rs = connection.execute(f"SELECT table_name column_name FROM information_schema.columns WHERE table_name LIKE '{table_prefix}_%%' order by table_name ASC")
        rows = [row[0] for row in rs]
        assert len(rows) == 100

        expected = sorted([f"{table_prefix}_{i}" for i in range(0, 100)])
        assert expected == rows
    finally:
        if not keep_data:
            for i in range(0, 100):
                table_name = table_prefix + "_" + str(i)
                logger.info('Dropping table %s in %s database ...', table_name, database.type)
                connection.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")

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
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 2, timeout_sec=300)
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
def test_dataflow_events(sdc_builder, sdc_executor, database):
    pytest.skip("No events supported in PostgreSQL Metadata Processor at this time.")


@database('postgresql')
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("PostgreSQL Metadata Processor doesn't deal with data formats")


@database('postgresql')
def test_push_pull(sdc_builder, sdc_executor, database):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")


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
    processor.set_attributes(table_name=destination_table_name)
    # Property omit_constraints_when_creating_tables fails when accesed using set_attributes
    processor.configuration["conf.omitConstraintsWhenCreatingTables"] = omit_constraints_when_creating_tables

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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
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
