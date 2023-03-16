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

import json
import logging
import string

import pytest
import sqlalchemy
from operator import itemgetter
from streamsets.testframework.markers import singlestore, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.singlestore, sdc_min_version('5.5.0')]

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Gastly'},
    {'id': 2, 'name': 'Haunter'},
    {'id': 3, 'name': 'Gengar'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Kyogre'},  # duplicated key
    {'id': 4, 'name': 'Groudon'}
]
RAW_DATA = ['id,name'] + [','.join(str(value) for value in row.values()) for row in ROWS_IN_DATABASE]


def _get_random_name(prefix='', length=5):
    """Generate a random string to use as a database object name.

    Args:
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    name = '{}{}'.format(prefix.lower(), get_random_string(string.ascii_lowercase))

    return name


def _create_table(table_name, singlestore):
    """Helper function to create a table with two columns: id (int, PK) and name (str).

    Args:
        table_name: (:obj:`str`) the name for the new table.
        singlestore: a :obj:`streamsets.testframework.environment.SingleStore` object.

    Return:
        The new table as a sqlalchemy.Table object.

    """
    metadata = sqlalchemy.MetaData()

    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('name', sqlalchemy.String(32)),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
                             )

    logger.info('Creating table %s ...', table_name)
    table.create(singlestore.engine)
    return table


def _create_singlestore_pipeline(pipeline_builder, pipeline_title, raw_data, table_name, operation):
    """Helper function to create and return a pipeline with SingleStore destination
    The Deduplicator assures there is only one ingest to database. The pipeline looks like:
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> singlestore
                               record_deduplicator >> trash
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    FIELD_MAPPINGS = [dict(field='/id', columnName='id'),
                      dict(field='/name', columnName='name')]
    singlestore_destination = pipeline_builder.add_stage('SingleStore')
    singlestore_destination.set_attributes(default_operation=operation,
                                           table_name=table_name,
                                           field_to_column_mapping=FIELD_MAPPINGS,
                                           stage_on_record_error='STOP_PIPELINE',
                                           use_fast_load=False)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> singlestore_destination
    record_deduplicator >> trash

    return pipeline_builder.build(title=pipeline_title)


@pytest.mark.parametrize('field_mapping', [True, False])
def test_singlestore_destination_no_implicit_mapping(sdc_builder, sdc_executor, singlestore, field_mapping):
    """This test covers situation when neither of the record fields matches the destination table - in such cases
    the record should ended up in error stream.
    """

    # Every second record have columns not available in the target table and should end up inside error stream
    INSERT_DATA = [
        {'id': 1, 'name': 'Gastly'},
        {'date_of_birth': 'yesterday'},
        {'id': 2, 'name': 'Haunter'},
        {'date_of_birth': 'tomorrow'},
        # These extra rows will actually hit the multi-row block of code
        {'id': 3, 'name': 'Haunter1'},
        {'id': 4, 'name': 'Haunter2'},
        {'id': 5, 'name': 'Haunter3'},
        {'id': 6, 'name': 'Haunter4'},

    ]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, singlestore)

    DATA = '\n'.join(json.dumps(rec) for rec in INSERT_DATA)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    FIELD_MAPPINGS = [dict(field='/id', columnName='id', dataType='INTEGER'),
                      dict(field='/name', columnName='name', dataType='STRING')] if field_mapping else []
    singlestore_destination = pipeline_builder.add_stage('SingleStore')
    singlestore_destination.set_attributes(default_operation='INSERT',
                                           table_name=table_name,
                                           field_to_column_mapping=FIELD_MAPPINGS,
                                           stage_on_record_error='TO_ERROR',
                                           use_fast_load=False)
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> [singlestore_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(singlestore)

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.error_records) == 2
        assert 'JDBC_90' == wiretap.error_records[0].header['errorCode']
        assert 'JDBC_90' == wiretap.error_records[1].header['errorCode']

        result = singlestore.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=itemgetter(1))  # order by id
        result.close()

        assert data_from_database == [(record['name'], record['id']) for record in INSERT_DATA if 'id' in record]
    finally:
        logger.info('Dropping table %s ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_destination_coerced_insert(sdc_builder, sdc_executor, singlestore):
    """Extension of the Simple SingleStore Destination test with INSERT operation.
    The pipeline inserts records into the database.
     In one record, data is represented as type String, where column is type Integer.
     This should be passed to the database to coerce.
     Verify that correct data is in the database.

     Please note the use of local COERCE_ROWS_IN_DATABASE to insert
     and global ROWS_IN_DATABASE to verify.

     COERCE_ has id (integer) set to string.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, singlestore)

    COERCE_ROWS_IN_DATABASE = [
        {'id': '1', 'name': 'Gastly'},
        {'id': '2', 'name': 'Haunter'},
        {'id': '3', 'name': 'Gengar'}
    ]

    DATA = '\n'.join(json.dumps(rec) for rec in COERCE_ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_singlestore_pipeline(pipeline_builder, 'SingleStore Destination Insert', DATA, table_name, 'INSERT')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(singlestore))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        result = singlestore.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=itemgetter(1))  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_destination_delete(sdc_builder, sdc_executor, singlestore):
    """Simple SingleStore Destination test with DELETE operation.
    The pipeline deletes records from the database and verify that correct data is in the database.
    Records are deleted if the primary key is matched irrespective of other column values.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, singlestore)
    logger.info('Adding %s rows into database ...', len(ROWS_IN_DATABASE))
    connection = singlestore.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_singlestore_pipeline(pipeline_builder, 'SingleStore Destination Delete', DATA, table_name, 'DELETE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(singlestore))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = singlestore.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        removed_ids = [record['id'] for record in ROWS_TO_UPDATE]
        assert sorted(data_from_database) == [(record['name'], record['id']) for record in ROWS_IN_DATABASE if
                                      record['id'] not in removed_ids]
    finally:
        logger.info('Dropping table %s ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_destination_update(sdc_builder, sdc_executor, singlestore):
    """Simple SingleStore Destination test with UPDATE operation.
    The pipeline updates records from the database and verify that correct data is in the database.
    Records with matching primary key are updated, and no action for unmatched records.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, singlestore)
    logger.info('Adding %s rows into the database ...', len(ROWS_IN_DATABASE))
    connection = singlestore.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_singlestore_pipeline(pipeline_builder, 'SingleStore Destination Update', DATA, table_name, 'UPDATE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(singlestore))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = singlestore.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=itemgetter(1))  # order by id
        result.close()
        updated_names = {record['id']: record['name'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['id']: record['name'] for record in ROWS_TO_UPDATE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_destination_with_duplicates(sdc_builder, sdc_executor, singlestore):
    """
    Make sure duplicate data related errors are send to error stream.
    """

    table_name = get_random_string(string.ascii_lowercase, 15)

    builder = sdc_builder.get_pipeline_builder()

    # Generate batch that will repeat the same primary key in the middle of the batch (on third row)
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"id" : 1}\n{"id" : 2}\n{"id" : 1}\n{"id" : 3}"""

    producer = builder.add_stage('SingleStore')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_fast_load = False

    source >> producer

    pipeline = builder.build().configure_for_environment(singlestore)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True)
    )
    try:
        logger.info('Creating table %s ...', table_name)
        table.create(singlestore.engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Since we are inserting duplicate primary key, the batch should fail
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 4
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 4
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0

        # And similarly the database side should be empty as well
        result = singlestore.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        assert len(data_from_database) == 0

    finally:
        logger.info('Dropping table %s...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_destination_multitable(sdc_builder, sdc_executor, singlestore):
    """Test for SingleStore Destination with multiple destination table. We create 3 tables and use an EL
    expression to insert records according to the /table record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> singlestore
                               record_deduplicator >> trash

    """
    table1_name = _get_random_name(prefix='stf_table_')
    table2_name = _get_random_name(prefix='stf_table_')
    table3_name = _get_random_name(prefix='stf_table_')

    table1 = _create_table(table1_name, singlestore)
    table2 = _create_table(table2_name, singlestore)
    table3 = _create_table(table3_name, singlestore)

    ROWS = [{'table': table1_name, 'id': 1, 'name': 'Gastly'},
            {'table': table2_name, 'id': 2, 'name': 'Haunter'},
            {'table': table3_name, 'id': 3, 'name': 'Gengar'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_singlestore_pipeline(pipeline_builder, 'SingleStore Destination Multitable Insert', INPUT_DATA,
                                              "${record:value('/table')}", 'INSERT')

    pipeline.configure_for_environment(singlestore)
    pipeline[2].set_attributes(table_name="${record:value('/table')}")

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS))
        sdc_executor.stop_pipeline(pipeline)

        result1 = singlestore.engine.execute(table1.select())
        result2 = singlestore.engine.execute(table2.select())
        result3 = singlestore.engine.execute(table3.select())

        data1 = result1.fetchall()
        data2 = result2.fetchall()
        data3 = result3.fetchall()

        assert data1 == [(ROWS[0]['name'], ROWS[0]['id'])]
        assert data2 == [(ROWS[1]['name'], ROWS[1]['id'])]
        assert data3 == [(ROWS[2]['name'], ROWS[2]['id'])]

        result1.close()
        result2.close()
        result3.close()

    finally:
        logger.info('Dropping tables %s, %s, %s...', table1_name, table2_name, table3_name)
        table1.drop(singlestore.engine)
        table2.drop(singlestore.engine)
        table3.drop(singlestore.engine)


def test_singlestore_destination_ordering(sdc_builder, sdc_executor, singlestore):
    """Ensure that variously intertwined operations won't be executed out of order in harmful way."""

    table_name = get_random_string(string.ascii_lowercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True, autoincrement=False),
        sqlalchemy.Column('a', sqlalchemy.Integer, quote=True),
        sqlalchemy.Column('b', sqlalchemy.Integer, quote=True)
    )

    RAW_DATA = [
        # Update id=5
        {"op": 3, "id": 5, "a": 2, "b": 2},
        # Insert id=4
        {"op": 1, "id": 4, "a": 1, "b": 1},
        # Update id=4
        {"op": 3, "id": 4, "a": 2, "b": 2},
        # Delete id=5
        {"op": 2, "id": 5},

        # Insert id=1
        {"op": 1, "id": 1, "a": 1, "b": 1},
        # Update id=1
        {"op": 3, "id": 1, "a": 2},
        # Insert id=2
        {"op": 1, "id": 2, "a": 1, "b": 1},
        # Delete id=2
        {"op": 2, "id": 2},
        # Update id=1
        {"op": 3, "id": 1, "a": 2, "b": 2},
        # Insert id=3
        {"op": 1, "id": 3, "a": 1, "b": 1},
        # Update id=1
        {"op": 3, "id": 1, "a": 3},
        # Update id=3
        {"op": 3, "id": 3, "a": 5},
        # Delete id=3
        {"op": 2, "id": 3}
    ]

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '\n'.join(json.dumps(rec) for rec in RAW_DATA)

    expression = builder.add_stage('Expression Evaluator')
    expression.header_attribute_expressions = [
        {'attributeToSet': 'sdc.operation.type', 'headerAttributeExpression': '${record:value("/op")}'}
    ]

    remover = builder.add_stage('Field Remover')
    remover.set_attributes(fields=['/op'], action='REMOVE')

    producer = builder.add_stage('SingleStore')
    producer.field_to_column_mapping = []
    producer.default_operation = 'UPDATE'
    producer.table_name = table_name
    producer.use_fast_load = False

    source >> expression >> remover >> producer

    pipeline = builder.build().configure_for_environment(singlestore)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s ...', table_name)
        table.create(singlestore.engine)

        # The table will start with single row (id=5)
        logger.info('Inserting rows into %s ', table_name)
        connection = singlestore.engine.connect()
        connection.execute(table.insert(), {'id': 5, 'a': 1, 'b': 1})

        # Finally run the pipeline and verify it's outcome
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = singlestore.engine.execute(table.select())
        db = sorted(result.fetchall(), key=itemgetter(0))  # order by id
        result.close()

        assert len(db) == 2

        # id=1
        assert 1 == db[0][0]
        assert 3 == db[0][1]
        assert 2 == db[0][2]

        # id=5
        assert 4 == db[1][0]
        assert 2 == db[1][1]
        assert 2 == db[1][2]
    finally:
        logger.info('Dropping table %s ...', table_name)
        table.drop(singlestore.engine)


def _setup_delimited_file(sdc_executor, tmp_directory, csv_records):
    """Setup csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    raw_data = "\n".join(csv_records)
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='csv')

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # Generate some batches/files.
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=5)

    return csv_records


@pytest.mark.parametrize('dyn_table', [False, True])
def test_error_handling_when_there_is_no_primary_key(sdc_builder, sdc_executor, singlestore, dyn_table):
    """
    SDC-12960. Updating table with no PK results in NPE

    Tests if a user friendly error will be added to record errors, one error for each failing record,
    instead of breaking the whole pipeline when UPDATE or DELETE operation is used and a destination table has no
    primary key.
    Covers cases with/without dynamic tables

    The pipeline:
    Dev Raw Data Source->Expression Evaluator->Field Remover->SingleStore

    The data source will send 2 insert records, 1 update and 1 delete record.
    The records also contain fields that define what table (if the dyamic table testing is enabled) to use;
    and what operation to perform on the data.

    The evaluator will use the control fields to set
    the sdc.operation.type attribute which defines what operation to perform;
    the tbl attribute which will be used in an EL to specify the destination table.

    The remover will remove the control fields from the records.

    And the producer does the actual data writing of records using their control attributes set by the evaluator.

    If the dynamic testing is enabled then the destination table name is defined by the record itself,
    this allows in one test to write data to different tables;
    if disabled, then all records will be send to the same destination table.

    We expect that if a destination table has no PRIMARY KEY, the delete and the update operations fail.
    That should not break the whole pipeline.
    Only the failed record should be marked as failed.
    And the error message should contain a description of the actual error and not the NPE as before.
    """

    table_names = [get_random_string(string.ascii_lowercase, 20) for _ in range(0, 4 if dyn_table else 1)]
    table_name_expression = ("${record:attribute('tbl')}") if dyn_table else table_names[0]
    metadata = sqlalchemy.MetaData()
    tables = [sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, quote=True, autoincrement=False),
        sqlalchemy.Column('operation', sqlalchemy.String(32), quote=True),
    ) for table_name in table_names]
    expected_error_record_count = 3 * len(tables)
    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.data_format = 'JSON'
    data_source.stop_after_first_batch = True
    data_source.raw_data = '\n'.join(['\n'.join(json.dumps(obj) for obj in [{
        "id": 1,
        "operation_header": "2",
        "operation": "delete",
        "table": table_name
    }, {
        "id": 2,
        "operation_header": "3",
        "operation": "update",
        "table": table_name
    }, {
        "id": 3,
        "operation_header": "3",
        "operation": "update",
        "table": table_name
    }]) for table_name in table_names])

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = []
    expression.header_attribute_expressions = [{
        "attributeToSet": "sdc.operation.type",
        "headerAttributeExpression": "${record:value('/operation_header')}"
    }, {
        "attributeToSet": "tbl",
        "headerAttributeExpression": "${record:value('/table')}"
    }]

    remover = builder.add_stage('Field Remover')
    remover.set_attributes(fields=['/operation_header', '/table'], action='REMOVE')

    producer = builder.add_stage('SingleStore')
    producer.field_to_column_mapping = []
    producer.default_operation = 'UPDATE'
    producer.table_name = table_name_expression
    producer.use_fast_load = False

    wiretap = builder.add_wiretap()
    data_source >> expression >> remover >> [producer, wiretap.destination]

    pipeline = builder.build().configure_for_environment(singlestore)

    pipeline.stages.get(label=producer.label).table_name = table_name_expression

    sdc_executor.add_pipeline(pipeline)

    created_table_count = 0  # We will remember how many tables we have actually succeeded to create
    connection = singlestore.engine.connect()
    try:
        for table in tables:
            # If for some crazy reason this fails
            # created_table_count will contain the number of tables we actually have succeeded to create
            table.create(singlestore.engine)
            created_table_count += 1
            connection.execute(table.insert(), [
                {'id': 1, 'operation': 'insert'},
                {'id': 2, 'operation': 'insert'},
                {'id': 3, 'operation': 'insert'}
            ])

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info('pipeline: %s', pipeline)
        assert expected_error_record_count == len(wiretap.error_records)
        for i in range(0, expected_error_record_count):
            assert 'JDBC_62' == wiretap.error_records[i].header['errorCode']
    finally:
        for i in range(0, created_table_count):  # We will drop only the tables we have created
            tables[i].drop(singlestore.engine)
        connection.close()


def test_fast_load(sdc_builder, sdc_executor, singlestore):
    """Test for SingleStore target stage with fast load. Data is inserted into SingleStore in the pipeline.
    Data is read from SingleStore using mysql client. We assert the data from the client to what has
    been ingested by the SingleStore pipeline.

    The pipeline looks like:
    SingleStore pipeline:
        dev_raw_data_source  >> SingleStore
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    table = _create_table_in_database(table_name, singlestore)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(RAW_DATA),
                                       stop_after_first_batch=True)

    singlestore_destination = pipeline_builder.add_stage('SingleStore')
    singlestore_destination.set_attributes(field_to_column_mapping=[],
                                           table_name=table_name,
                                           use_fast_load=True)

    dev_raw_data_source >> singlestore_destination

    pipeline = pipeline_builder.build(title='SingleStore Fast Load').configure_for_environment(singlestore)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        result = singlestore.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]

    finally:
        logger.info('Dropping table %s in SingleStore database ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_ignore_duplicate(sdc_builder, sdc_executor, singlestore):
    """Test records with duplicate keys. The existing records should be kept with IGNORE option.

    The pipeline looks like:
        dev_raw_data_source >> singlestore
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE),
                                       stop_after_first_batch=True)

    singlestore_destination = pipeline_builder.add_stage('SingleStore')
    singlestore_destination.set_attributes(duplicate_key_error_handling='IGNORE',
                                           field_to_column_mapping=[],
                                           table_name=table_name,
                                           use_fast_load=True)

    dev_raw_data_source >> singlestore_destination

    pipeline = pipeline_builder.build(title='SingleStore Ignore Duplicate').configure_for_environment(singlestore)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, singlestore)
    connection = singlestore.engine.connect()
    try:
        logger.info('Adding %s rows into SingleStore database ...', len(ROWS_IN_DATABASE))
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = connection.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id

        updated_names = {record['id']: record['name'] for record in ROWS_TO_UPDATE}
        updated_names.update({record['id']: record['name'] for record in ROWS_IN_DATABASE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in data_from_database]
    finally:
        connection.close()
        logger.info('Dropping table %s in SingleStore database ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_replace_duplicate(sdc_builder, sdc_executor, singlestore):
    """Test records with duplicate keys. The existing records should be replaced with REPLACE option.

    The pipeline looks like:
        dev_raw_data_source >> singlestore
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE),
                                       stop_after_first_batch=True)

    singlestore_destination = pipeline_builder.add_stage('SingleStore')
    singlestore_destination.set_attributes(duplicate_key_error_handling='REPLACE',
                                           field_to_column_mapping=[],
                                           table_name=table_name,
                                           use_fast_load=True)

    dev_raw_data_source >> singlestore_destination

    pipeline = pipeline_builder.build(title='SingleStore Replace Duplicate').configure_for_environment(singlestore)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, singlestore)
    connection = singlestore.engine.connect()
    try:
        logger.info('Adding %s rows into SingleStore database ...', len(ROWS_IN_DATABASE))
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = connection.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id

        updated_names = {record['id']: record['name'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['id']: record['name'] for record in ROWS_TO_UPDATE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in data_from_database]
    finally:
        connection.close()
        logger.info('Dropping table %s in SingleStore database ...', table_name)
        table.drop(singlestore.engine)


def test_singlestore_invalid_ops(sdc_builder, sdc_executor, singlestore):
    """Test records with invalid operations. SingleStore Fast Loader does not support CDC origins which
    contains sdc.operation.type in the header. Expression Evaluator is used to add the header
    attributes. Records with any operation type should go to error records.

    The pipeline looks like:
        dev_raw_data_source >> expression evaluator >> singlestore
    """
    table_name = get_random_string(string.ascii_lowercase, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Haunter', 'id': 2},  # delete
        {'operation': 3, 'name': 'Kyogre', 'id': 3},  # update
        {'operation': 1, 'name': 'Groudon', 'id': 4}  # insert
    ]
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in DATA),
                                       stop_after_first_batch=True)

    HEADER_EXPRESSIONS = [dict(attributeToSet='sdc.operation.type',
                               headerAttributeExpression="${record:value('/operation')}")]
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = HEADER_EXPRESSIONS

    singlestore_destination = pipeline_builder.add_stage('SingleStore')
    singlestore_destination.set_attributes(duplicate_key_error_handling='REPLACE',
                                           field_to_column_mapping=[],
                                           table_name=table_name,
                                           use_fast_load=True)

    dev_raw_data_source >> expression_evaluator >> singlestore_destination

    pipeline = pipeline_builder.build(title='SingleStore Invalid Ops').configure_for_environment(singlestore)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, singlestore)
    connection = singlestore.engine.connect()
    try:
        logger.info('Adding %s rows into SingleStore database ...', len(ROWS_IN_DATABASE))
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.SingleStore_01.inputRecords.counter').count == len(DATA)
        assert history.latest.metrics.counter('stage.SingleStore_01.errorRecords.counter').count == len(DATA)

        result = connection.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        connection.close()
        logger.info('Dropping table %s in SingleStore database ...', table_name)
        table.drop(singlestore.engine)


def _create_table_in_database(table_name, singlestore):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('name', sqlalchemy.String(32)),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    )
    logger.info('Creating table %s in SingleStore database ...', table_name)
    table.create(singlestore.engine)
    return table

