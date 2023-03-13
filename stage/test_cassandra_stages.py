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

import json
import logging
import pytest
import string

from streamsets.sdk.sdc_api import RunError, RunningError, StartError
from streamsets.testframework.markers import cassandra, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_TABLE_NAME = 'contact'

SINGLE_RAW_DICT = [
    dict(
        contact=dict(
            name='Sansa',
            phone=2120298998,
            zip_code=16305
        )
    )
]
SINGLE_ROW_JSON_DUMP = json.dumps(SINGLE_RAW_DICT)

DEFAULT_RAW_DICT = [
    dict(
        contact=dict(
            name='Jane Smith',
            phone=2124050000,
            zip_code=27023
        )
    ),
    dict(
        contact=dict(
            name='San',
            phone=2120998998,
            zip_code=14305
        )
    ),
    dict(
        contact=dict(
            name='Sansa',
            phone=2120298998,
            zip_code=16305
        )
    )
]
DEFAULT_JSON_DUMP = json.dumps(DEFAULT_RAW_DICT)

DEFAULT_COLUMNS = [('name', 'text', True), ('zip_code', 'int'), ('phone', 'int')]
DEFAULT_COLUMN_MAPPINGS = [
    {'field': '/contact/name', 'columnName': 'name'},
    {'field': '/contact/zip_code', 'columnName': 'zip_code'},
    {'field': '/contact/phone', 'columnName': 'phone'}
]


def _set_up_cassandra_authentication(cassandra, cassandra_destination):
    if cassandra.kerberos_enabled:
        cassandra_destination.set_attributes(
            authentication_provider='KERBEROS'
        )
    else:
        cassandra_destination.set_attributes(
            authentication_provider='PLAINTEXT',
            password=cassandra.password,
            username=cassandra.username
        )


def _create_default_keyspace(session, keyspace):
    session.execute(
        f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )


def _create_table(session, keyspace, table=DEFAULT_TABLE_NAME, columns=DEFAULT_COLUMNS):
    columns_definition = _create_columns_definition(columns)
    session.execute(f'CREATE TABLE {keyspace}.{table} ({columns_definition})')


def _create_columns_definition(columns):
    """
    The expected format of the columns parameter is as follows:
        [(column_1_name, column_1_type[, is_primary_key]), (column_2_name, column_2_type[, is_primary_key]), ... ]

    If the field is_primary_key is missing, this method assumes the column is not a primary key. This method is not
    prepared to set up a compound primary key.
    """
    columns_definition = ''
    for column in columns:
        if columns_definition is not '':
            columns_definition += ', '

        columns_definition += f'{column[0]} {column[1]}'
        if len(column) == 3 and column[2]:
            columns_definition += ' PRIMARY KEY'

    return columns_definition


def _get_current_rows(session, keyspace, table=DEFAULT_TABLE_NAME):
    return session.execute(f'SELECT * FROM {keyspace}.{table}').current_rows


def _assert_table_rows_values(rows, raw_data, table=DEFAULT_TABLE_NAME):
    assert len(rows) == len(raw_data), f'{len(raw_data)} rows were expected, but {len(rows)} were found'

    def sort_raw_data(entry):
        return entry[table]['name']

    def sort_rows(entry):
        return entry[0]

    raw_data.sort(key=sort_raw_data)
    rows.sort(key=sort_rows)

    for index, row in enumerate(rows):
        for key, value in raw_data[index][table].items():
            assert getattr(row, key) == value, \
                f'The expected value of the field {key} was {value} but {getattr(row, key)} was found'


def _drop_table(session, keyspace, table=DEFAULT_TABLE_NAME):
    session.execute(f'DROP TABLE IF EXISTS {keyspace}.{table}')


def _drop_keyspace(session, keyspace):
    session.execute(f'DROP KEYSPACE IF EXISTS {keyspace}')


def _start_pipeline_and_check_on_error_record_discard(sdc_executor, pipeline):
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('stage.Cassandra_01.outputRecords.counter').count == 1
    assert history.latest.metrics.counter('stage.Cassandra_01.errorRecords.counter').count == 0


def _start_pipeline_and_check_on_error_record_stops(sdc_executor, pipeline):
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, "The pipeline should have arisen an exception after the error"
    except (RunError, RunningError) as e:
        response = sdc_executor.get_pipeline_status(pipeline).response.json()
        status = response.get('status')
        logger.info('Pipeline status %s ...', status)
        assert 'CASSANDRA_06' in e.message


def _start_pipeline_and_check_on_error_record_to_error(sdc_executor, pipeline):
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('stage.Cassandra_01.outputRecords.counter').count == 0
    assert history.latest.metrics.counter('stage.Cassandra_01.errorRecords.counter').count == 1


@cassandra
@pytest.mark.parametrize('enable_batches', [True, False])
@pytest.mark.parametrize(
    'raw_data, json_dump',
    [(SINGLE_RAW_DICT, SINGLE_ROW_JSON_DUMP), (DEFAULT_RAW_DICT, DEFAULT_JSON_DUMP)]
)
def test_cassandra_destination_enable_batches(
        sdc_builder,
        sdc_executor,
        cassandra,
        enable_batches,
        raw_data,
        json_dump
):
    """
    Test that the Cassandra destination works with and without batches enabled.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json_dump,
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=DEFAULT_COLUMN_MAPPINGS,
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4',
        enable_batches=enable_batches
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(session, cassandra_keyspace)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows = _get_current_rows(session, cassandra_keyspace)
        _assert_table_rows_values(rows, raw_data)
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
@pytest.mark.parametrize('enable_batches', [True, False])
def test_cassandra_destination_empty_batch(sdc_builder, sdc_executor, cassandra, enable_batches):
    """
    Test that the Cassandra destination stage works as expected with empty batches.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data="[]",
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=DEFAULT_COLUMN_MAPPINGS,
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4',
        enable_batches=enable_batches
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(session, cassandra_keyspace)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows = _get_current_rows(session, cassandra_keyspace)

        assert len(rows) == 0
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
@pytest.mark.parametrize('num_records, max_batch_size', [[10, 2], [100, 30], [100, 100], [50, 100]])
def test_cassandra_destination_internal_sub_batching(
        sdc_builder,
        sdc_executor,
        cassandra,
        num_records,
        max_batch_size
):
    """
    Test that the Cassandra destination internal sub-batching works as expected when there are more records than fit a
    batch, when there are as many records as allowed in a single batch and when there are fewer records than fit in a
    single batch.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    raw_data = [dict(contact=dict(name='Jane Smith', age=age)) for age in range(num_records)]

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json.dumps(raw_data),
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=[
            {'field': '/contact/name', 'columnName': 'name'},
            {'field': '/contact/age', 'columnName': 'age'}
        ],
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4',
        max_batch_size=max_batch_size
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(session, cassandra_keyspace, DEFAULT_TABLE_NAME, [('name', 'text'), ('age', 'int', True)])

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows = _get_current_rows(session, cassandra_keyspace)
        _assert_table_rows_values(rows, raw_data)
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
def test_cassandra_destination_collection_types(sdc_builder, sdc_executor, cassandra):
    """
    Test that the Cassandra destination stage works with different collection types (maps and lists).

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    raw_data = [dict(
        contact=dict(
            name='Jane Smith',
            preferred_food=['pizza', 'smoothie', 'yogurt'],
            extra_information={'phone': '926385278', 'mail': 'jane@gmail.com'}
        )
    )]

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json.dumps(raw_data),
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=[
            {'field': '/contact/name', 'columnName': 'name'},
            {'field': '/contact/preferred_food', 'columnName': 'preferred_food'},
            {'field': '/contact/extra_information', 'columnName': 'extra_information'}
        ],
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4'
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(
            session,
            cassandra_keyspace,
            DEFAULT_TABLE_NAME,
            [('name', 'text', True), ('preferred_food', 'list<text>'), ('extra_information', 'map<text,text>')]
        )

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows = _get_current_rows(session, cassandra_keyspace)

        assert len(rows) == len(raw_data)
        _assert_table_rows_values(rows, raw_data)
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
def test_cassandra_destination_record_with_missing_fields(
        sdc_builder,
        sdc_executor,
        cassandra
):
    """
    Test that the Cassandra destination does not arise any errors if a field is missing from a record.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    # Create a record without the field 'zip_code'
    raw_data = [
        dict(
            contact=dict(
                name='Sansa',
                phone=2120298998
            )
        )
    ]
    json_data = json.dumps(raw_data)
    expected_value = [{'contact': {'name': 'Sansa', 'phone': 2120298998, 'zip_code': None}}]

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json_data,
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=DEFAULT_COLUMN_MAPPINGS,
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4'
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(session, cassandra_keyspace)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows = _get_current_rows(session, cassandra_keyspace)

        assert len(rows) == len(expected_value)
        _assert_table_rows_values(rows, expected_value)
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
@pytest.mark.parametrize(
    'on_record_error, start_and_check_pipeline_behaviour',
    [
        ('DISCARD', _start_pipeline_and_check_on_error_record_discard),
        ('STOP_PIPELINE', _start_pipeline_and_check_on_error_record_stops),
        ('TO_ERROR', _start_pipeline_and_check_on_error_record_to_error)
    ]
)
def test_cassandra_destination_errors(
        sdc_builder,
        sdc_executor,
        cassandra,
        on_record_error,
        start_and_check_pipeline_behaviour
):
    """
    Check the behaviour of the Cassandra destination with all the On Record Error options.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    raw_data = [dict(contact=dict(name='Jane Smith', age='x'))]
    json_data = json.dumps(raw_data)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json_data,
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=[
            {'field': '/contact/name', 'columnName': 'name'},
            {'field': '/contact/age', 'columnName': 'age'}
        ],
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4',
        on_record_error=on_record_error
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(session, cassandra_keyspace, DEFAULT_TABLE_NAME, [('name', 'text', True), ('age', 'int')])

        start_and_check_pipeline_behaviour(sdc_executor, pipeline)
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
@pytest.mark.parametrize(
    'cassandra_keyspace, table_name',
    [
        (get_random_string(string.ascii_letters, 10), DEFAULT_TABLE_NAME),
        (get_random_string(string.ascii_letters, 10), f"{pytest.param('cassandra_keyspace')}_{DEFAULT_TABLE_NAME}"),
        (get_random_string(string.ascii_letters, 10), f"{pytest.param('cassandra_keyspace')},{DEFAULT_TABLE_NAME}"),
        (get_random_string(string.ascii_letters, 10), f"{pytest.param('cassandra_keyspace')}#{DEFAULT_TABLE_NAME}"),
        (get_random_string(string.ascii_letters, 10), f"{pytest.param('cassandra_keyspace')}:{DEFAULT_TABLE_NAME}")
    ]
)
def test_cassandra_destination_malformed_table_name(
        sdc_builder,
        sdc_executor,
        cassandra,
        cassandra_keyspace,
        table_name
):
    """
    Test that the Cassandra destination throws a StageException when the table name does not follow the pattern
    <key space>.<table_name>.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=DEFAULT_JSON_DUMP,
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=DEFAULT_COLUMN_MAPPINGS,
        fully_qualified_table_name=table_name,
        protocol_version='V4'
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)
        _create_table(session, cassandra_keyspace)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, "The pipeline should have arisen an exception after the error"
    except StartError as e:
        response = sdc_executor.get_pipeline_status(pipeline).response.json()
        status = response.get('status')
        logger.info('Pipeline status %s ...', status)
        assert 'CASSANDRA_02' in e.message
    finally:
        _drop_table(session, cassandra_keyspace)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()


@cassandra
def test_cassandra_destination_non_existing_table(
        sdc_builder,
        sdc_executor,
        cassandra
):
    """
    Test that the Cassandra destination throws a StageException when there is no table identified by the provided name.

    The pipeline looks like:
    dev_raw_data_source >> cassandra_destination
    """
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=DEFAULT_JSON_DUMP,
        stop_after_first_batch=True
    )

    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(
        field_to_column_mapping=DEFAULT_COLUMN_MAPPINGS,
        fully_qualified_table_name=f'{cassandra_keyspace}.{DEFAULT_TABLE_NAME}',
        protocol_version='V4'
    )
    _set_up_cassandra_authentication(cassandra, cassandra_destination)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    client = cassandra.client
    cluster = client.cluster
    session = client.session

    try:
        _create_default_keyspace(session, cassandra_keyspace)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, "The pipeline should have arisen an exception after the error"
    except StartError as e:
        response = sdc_executor.get_pipeline_status(pipeline).response.json()
        status = response.get('status')
        logger.info('Pipeline status %s ...', status)
        assert 'CASSANDRA_12' in e.message
    finally:
        _drop_table(session, cassandra_keyspace, DEFAULT_TABLE_NAME)
        _drop_keyspace(session, cassandra_keyspace)
        cluster.shutdown()
