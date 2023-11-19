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

import json
import logging
import datetime
from decimal import Decimal
from string import ascii_lowercase
import pytz

import pytest
from . import _clean_up_bigquery, _clean_up_gcs
from google.cloud.bigquery import SchemaField, Table, DatasetReference
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

DESTINATION_STAGE_NAME = 'com_streamsets_pipeline_stage_bigquery_enterprise_destination_BigQueryDTarget'

logger = logging.getLogger(__name__)

pytestmark = [gcp, sdc_min_version('5.3.0'), pytest.mark.category('nonstandard')]

# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'
PRIMARY_KEY_SPECIFICATION = 'jdbc.primaryKeySpecification'

SCHEMA = [SchemaField('title', 'STRING'),
          SchemaField('author', 'STRING'),
          SchemaField('genre', 'STRING'),
          SchemaField('publisher', 'STRING')]

ROWS_IN_DATABASE = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future",
     "author": "Ashlee Vance",
     "genre": "Biography",
     "publisher": "HarperCollins Publishers"},
    {"TITLE": "Europe, Through the Back Door",
     "AUTHOR": "Rick Steves",
     "GENRE": "Travel",
     "PUBLISHER": "Rick Steves"},
    {"title": "Steve Jobs",
     "author": "Walter Isaacson",
     "genre": "Biography",
     "publisher": "Simon & Schuster"},
    {"title": "The Spy and the Traitor: The Greatest Espionage Story of the Cold War",
     "author": "Ben Macintyre",
     "genre": "Biography True crime",
     "publisher": "McClelland & Stewart"}
]

ROWS_IN_DATABASE_WITH_ERROR = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future",
     "author": "Ashlee Vance",
     "genre": "Biography",
     "publisher": "HarperCollins Publishers"},
    {"foo": "bar"},
    {"foo": "bar"},
    {"title": "The Spy and the Traitor: The Greatest Espionage Story of the Cold War",
     "author": "Ben Macintyre",
     "genre": "Biography True crime",
     "publisher": "McClelland & Stewart"}
]

NESTED_MAP_TO_LOAD = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future", "publishers":
        {"publisher1": "HarperCollins", "publisher2": "JB"}}
]

EXPECTED_AVRO_MAP_IN_DATABASE = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future",
     "publishers": [{'key': 'publisher1', 'value': 'HarperCollins'}, {'key': 'publisher2', 'value': 'JB'}]}
]

NESTED_LIST_TO_LOAD = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future", "publishers": ["HarperCollins", "JB"]}
]

NESTED_DRIFT_TO_LOAD = [
    {"title": "Europe, Through the Back Door", "publishers": ["Rick Steves"], "readers": ["JB", "no one else"]}
]

JSON_NESTED_MAP_TO_LOAD = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future", "publishers":
        {"publisher1": "HarperCollins", "publisher2": "JB", "inner_map": {"this_is_now": "working"}}}
]

JSON_NESTED_LIST_OF_MAPS_TO_LOAD = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future", "publishers":
        {"publisher1": "HarperCollins",
         "publisher2": "JB", "inner_list": [{"this_is_now": "working", "and_before": "it didn't"}]}}
]

JSON_NESTED_LIST_OF_LISTS_OF_MAPS_TO_LOAD = [
    {"title": "No more Elon", "publishers":
        {"publisher1": "HarperCollins", "publisher2": "JB",
         "inner_map": {"another_map": {"inner_string": "JB",
                                       "inner_list1": [{"inner_key": "inner_value1"},
                                                       {"inner_key": "inner_value2"}]}}}}
]

JSON_NESTED_LIST_OF_LISTS_OF_MAPS_WITH_NULLS_TO_LOAD = [
    {"title": "No more Elon", "publishers":
        {"publisher1": "HarperCollins", "publisher2": "JB",
         "inner_map": {"another_map": {"inner_string": "JB",
                                       "inner_list1": [{"inner_key1": "inner_value1"},
                                                       {"inner_key2": "inner_value2"}],
                                       "inner_map2": {"and_another_one": "some data"}}}}}
]

EXPECTED_JSON_NESTED_LIST_OF_LISTS_OF_MAPS_WITH_NULLS = [
    {"title": "No more Elon", "publishers":
        {"publisher1": "HarperCollins", "publisher2": "JB",
         "inner_map": {"another_map": {"inner_string": "JB",
                                       "inner_list1": [{"inner_key1": "inner_value1", 'inner_key2': None},
                                                       {"inner_key1": None, "inner_key2": "inner_value2"}],
                                       "inner_map2": {"and_another_one": "some data"}}}}}
]

# we need to add the empty list in a previous non-existent field (we call X but could be whatever)
EXPECTED_NESTED_LIST_WITH_DRIFT = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future", "publishers": ["HarperCollins", "JB"], "X": []},
    {"title": "Europe, Through the Back Door", "publishers": ["Rick Steves"], "readers": ["JB", "no one else"]}
]

ROWS_IN_DATABASE_QUOTING = [
    {"title": "\"Elon Musk: Tesla, SpaceX, the Quest for a Fantastic Future\"",
     "author": "Alex",
     "genre": "Escalation",
     "publisher": "StreamSets"}
]

ROWS_IN_DATABASE_NULL = [
    {"title": "Elon Musk: Tesla, SpaceX, the Quest for a Fantastic Future",
     "author": "Joaquin",
     "genre": "NULL",
     "publisher": "StreamSets"}
]

ROWS_IN_DATABASE_NULL_VALUE = [
    {"title": "Elon Musk: Tesla, SpaceX, the Quest for a Fantastic Future",
     "author": None,
     "genre": None,
     "publisher": "StreamSets"}
]

ROWS_IN_DATABASE_QUOTED_NEWLINES = [
    {"title": "Elon Musk: Tesla, SpaceX\n the Quest for a Fantastic Future",
     "author": "Joaquin",
     "genre": "Escalation",
     "publisher": "StreamSets"}
]

CDC_ROWS_IN_DATABASE = [
    {'OP': 1, 'NAME': 'Alex Sanchez', 'ROLE': 'Developer', 'AGE': 27, 'TEAM': 'Cloud'},
    {'OP': 1, 'NAME': 'Joaquin Bo', 'ROLE': 'Developer', 'AGE': 23, 'TEAM': 'Data plane'},
    {'OP': 1, 'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'Connectivity BCN'},
    {'OP': 1, 'NAME': 'Random person', 'ROLE': 'Prospect', 'AGE': 99, 'TEAM': 'Undefined'},
    {'OP': 1, 'NAME': 'Tucu', 'ROLE': 'Distinguished Developer', 'AGE': 50, 'TEAM': 'Innovation'},
    {'OP': 2, 'NAME': 'Random person'},  # Remove prospect
    {'OP': 4, 'NAME': 'Alex Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},  # Upsert Role and Team
    {'OP': 3, 'NAME': 'Joaquin Bo', 'ROLE': 'Developer', 'AGE': 24, 'TEAM': 'Collector'},  # Update Age and Team
    {'OP': 3, 'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'BCN'}  # Update Team
]

CDC_RESULT_ROWS = [
    {'NAME': 'Alex Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},
    {'NAME': 'Joaquin Bo', 'ROLE': 'Developer', 'AGE': 24, 'TEAM': 'Collector'},
    {'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'BCN'},
    {'NAME': 'Tucu', 'ROLE': 'Distinguished Developer', 'AGE': 50, 'TEAM': 'Innovation'},
]

CDC_ROWS_IN_DATABASE_QUOTING = [
    {'OP': 1, 'NAME': 'Alex" Sanchez', 'ROLE': 'Developer', 'AGE': 27, 'TEAM': 'Cloud'},
    {'OP': 1, 'NAME': 'Joaquin" Bo', 'ROLE': 'Developer', 'AGE': 23, 'TEAM': 'Data plane'},
    {'OP': 1, 'NAME': 'Martin Bal"zamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'Co"nnectivity BCN'},
    {'OP': 1, 'NAME': 'Random person', 'ROLE': 'Prospect', 'AGE': 99, 'TEAM': 'Undefined'},
    {'OP': 1, 'NAME': 'Tucu', 'ROLE': 'Distinguish"ed Developer', 'AGE': 50, 'TEAM': 'Innovation'},
    {'OP': 2, 'NAME': 'Random person'},  # Remove prospect
    {'OP': 4, 'NAME': 'Alex" Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},  # Upsert Role and Team
    {'OP': 3, 'NAME': 'Joaquin" Bo', 'ROLE': 'Developer', 'AGE': 24, 'TEAM': 'Collector'},  # Update Age and Team
    {'OP': 3, 'NAME': 'Martin Bal"zamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'B"CN'}  # Update Team
]

CDC_RESULT_ROWS_QUOTING = [
    {'NAME': 'Alex" Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},
    {'NAME': 'Joaquin" Bo', 'ROLE': 'Developer', 'AGE': 24, 'TEAM': 'Collector'},
    {'NAME': 'Martin Bal"zamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'B"CN'},
    {'NAME': 'Tucu', 'ROLE': 'Distinguish"ed Developer', 'AGE': 50, 'TEAM': 'Innovation'},
]

SCHEMA_FOR_DRIFT = [SchemaField('id', 'INTEGER'),
                    SchemaField('name', 'STRING')]

ROWS_FOR_DRIFT = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 3, 'name': 'Dominic Thiem'}
]

ROWS_FOR_DRIFT_STRING = [
    {'id': '1', 'name': 'Roger Federer'},
    {'id': '2', 'name': 'Rafael Nadal'},
    {'id': '3', 'name': 'Dominic Thiem'}
]

ROWS_FOR_DRIFT_NONE_COLUMN = [
    {'id': 1, 'name': 'Roger Federer', 'ranking': None},
    {'id': 2, 'name': 'Rafael Nadal', 'ranking': None},
    {'id': 3, 'name': 'Dominic Thiem', 'ranking': None}
]

ROWS_FOR_DRIFT_NONE_COLUMN_STRING = [
    {'id': '1', 'name': 'Roger Federer', 'ranking': None},
    {'id': '2', 'name': 'Rafael Nadal', 'ranking': None},
    {'id': '3', 'name': 'Dominic Thiem', 'ranking': None}
]

ROWS_FOR_DRIFT_EXT = [
    {'id': 4, 'name': 'Arthur Ashe', 'ranking': 1},
    {'id': 5, 'name': 'Ivan Lendl', 'ranking': 2},
    {'id': 6, 'name': 'Guillermo Vilas', 'ranking': 12}
]

ROWS_FOR_DRIFT_EXT_STRING = [
    {'id': 4, 'name': 'Arthur Ashe', 'ranking': '1'},
    {'id': 5, 'name': 'Ivan Lendl', 'ranking': '2'},
    {'id': 6, 'name': 'Guillermo Vilas', 'ranking': '12'}
]

ROWS_FOR_DRIFT_EXT_FULL_STRING = [
    {'id': '4', 'name': 'Arthur Ashe', 'ranking': '1'},
    {'id': '5', 'name': 'Ivan Lendl', 'ranking': '2'},
    {'id': '6', 'name': 'Guillermo Vilas', 'ranking': '12'}
]

DATA_TYPES = {
    # gcp_data_type, origin_data_type, origin_data, gcp_expected_data
    'INTEGER': ('INTEGER', 2424, 2424),
    'FLOAT': ('FLOAT', 2424.2424, 2424.2424),
    'BOOLEAN': ('BOOLEAN', True, True),
    # STRUCT and GEOGRAPHY do not actually support String defaults, so we expect NULLs coming from BQ
    'STRUCT': ('STRING', None, None),
    'GEOGRAPHY': ('STRING', None, None),
    'BYTES': ('BYTE_ARRAY', 'dataAsBytes', b'u\xabZ\x02\xc0r\xb5\xeb'),
    'DECIMAL': ('DECIMAL', -123456789.12345, Decimal('-123456789.12345')),
    'DATE': ('DATE', '2020-01-01', datetime.date(2020, 1, 1)),
    'DATETIME': ('DATETIME', '2019-02-05 23:59:59', datetime.datetime(2019, 2, 5, 23, 59, 59)),
    'TIME': ('TIME', '10:00:00',  datetime.time(10, 0)),
    'TIMESTAMP': ('TIME', '2019-02-05 23:59:59',  datetime.datetime(2019, 2, 5, 23, 59, 59, tzinfo=pytz.utc)),
    'STRING': ('STRING', 'gcp standard test 123', 'gcp standard test 123'),
}

CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER = [
    {
        'sdc.operation.type': 1,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 2,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 2,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit - Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit - Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 4,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'
    }
]

CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY = [
    {
        'TYPE': 'Hobbit',
        'ID': 1,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 0'
    }, {
        'TYPE': 'Fallohide',
        'ID': 1,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 1'
    }, {
        'TYPE': 'Fallohide',
        'ID': 2,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 2'
    }, {
        'TYPE': 'Hobbit - Fallohide',
        'ID': 3,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 3'
    }, {
        'TYPE': 'Hobbit, Fallohide',
        'ID': 3,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 4'
    }, {
        'TYPE': 'Hobbit, Fallohide',
        'ID': 4,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 5'
    }, {
        'TYPE': 'Hobbit, Fallohide',
        'ID': 4,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 6'
    }
]


@pytest.mark.parametrize('file_format', ['CSV', 'AVRO', 'JSON'])
def test_basic(sdc_builder, sdc_executor, gcp, file_format):
    """Test for Google BigQuery with Google Cloud Storage staging.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            staging_file_format=file_format,
                            enable_data_drift=False,
                            create_table=False,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s and table %s using Google BigQuery client ...', dataset_name, table_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('file_format', ['CSV', 'AVRO', 'JSON'])
@pytest.mark.parametrize('bigquery_create_schema', [True, False])
def test_basic_with_bigquery_schema_generator(sdc_builder, sdc_executor, gcp, file_format, bigquery_create_schema):
    """Test for Google BigQuery with Google Cloud Storage staging.
    Note that auto creating tables for this test is different, as BigQuery created tables on its own (BUT not datasets!)
    But in this test we also try creating them beforehand, to see it works even if we don't let BigQuery that job.

    When BigQuery is generating the schema, the names of the columns are string_field_x. So we should
    either create the table like that, or leave the creation of the table to BigQuery.
    (https://cloud.google.com/bigquery/docs/schema-detect#csv_header)

    If using BigQuery schema generation, mixing uppercase and lowercase in field names in Avro format
    will also fail, as we cannot format records using the schema that BigQuery generates later, and
    Avro doesn't accept mixed case fields (they are different fields).

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec).lower() for rec in ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='BIGQUERY',
                            staging_file_format=file_format,
                            enable_data_drift=False,
                            create_table=False,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        if not bigquery_create_schema:
            if file_format == 'CSV':
                schema = [SchemaField('string_field_0', 'STRING'),
                          SchemaField('string_field_1', 'STRING'),
                          SchemaField('string_field_2', 'STRING'),
                          SchemaField('string_field_3', 'STRING')]
            else:
                schema = SCHEMA
            logger.info('Creating table %s using Google BigQuery client ...', table_name)
            bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v.lower() for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_bigquery) == len(expected_data)

        if bigquery_create_schema and file_format == 'JSON':
            # When bigquery creates the columns, the columns order is unkown
            # thats is the reason for sorting each row
            assert [sorted(row) for row in data_from_bigquery] == \
                   [sorted(row) for row in expected_data]
        else:
            assert data_from_bigquery == expected_data

    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('auto_create_dataset', [True, False])
@pytest.mark.parametrize('auto_create_table', [True, False])
@pytest.mark.parametrize('create_new_columns_string', [True, False])
@pytest.mark.parametrize('new_columns_mode', ["REQUIRED", "NULLABLE"])
def test_data_drift(sdc_builder, sdc_executor, gcp, auto_create_dataset, auto_create_table, create_new_columns_string,
                    new_columns_mode):
    """Test for Google BigQuery with Google Cloud Storage staging using datadrift and its options, which are auto
    creating table, setting new columns as strings, and setting new columns bigquery modes.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_FOR_DRIFT)
    drift_data = '\n'.join(json.dumps(rec) for rec in ROWS_FOR_DRIFT_EXT)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=auto_create_table,
                            create_dataset=auto_create_dataset,
                            create_new_columns_as_string=create_new_columns_string,
                            new_columns_mode=new_columns_mode,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    # Then, we create the second pipeline, which will try to add new columns to the first table result
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=drift_data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=auto_create_table,
                            create_dataset=auto_create_dataset,
                            create_new_columns_as_string=create_new_columns_string,
                            new_columns_mode=new_columns_mode,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline_2 = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        # auto_create_dataset needs to have auto_create_table, because you need to create tables inside the new dataset
        # that is managed by the library itself (disabling auto_create_dataset when auto_create_table is disabled),
        # but we need to create the dataset also if the auto_create_table is false
        if not auto_create_dataset or not auto_create_table:
            logger.info(f'Creating dataset {dataset_name} ...')
            bigquery_client.create_dataset(dataset_ref)

        if not auto_create_table:
            logger.info(f'Creating table {table_name} ...')
            bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA_FOR_DRIFT))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        if auto_create_table and create_new_columns_string:
            # If auto creating with new columns as string, the id values are strings as well
            expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_STRING]
        else:
            expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data

        # Then, we run the second pipeline
        sdc_executor.add_pipeline(pipeline_2)
        sdc_executor.start_pipeline(pipeline_2).wait_for_finished()

        # And Verify again with the new data
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        if auto_create_table and create_new_columns_string:
            # If auto creating with new columns as string, the id values are strings as well
            expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_NONE_COLUMN_STRING]
            expected_data += [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_EXT_FULL_STRING]
        else:
            # Else, the id values are integers
            expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_NONE_COLUMN]
            if create_new_columns_string:
                # But additional created columns are strings if set
                expected_data += [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_EXT_STRING]
            else:
                expected_data += [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_EXT]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('auto_create_dataset', [True, False])
@pytest.mark.parametrize('auto_create_table', [True, False])
@pytest.mark.parametrize('new_columns_mode', ["REQUIRED", "NULLABLE"])
def test_data_drift_avro(sdc_builder, sdc_executor, gcp, auto_create_dataset, auto_create_table, new_columns_mode):
    """Test for Google BigQuery with Google Cloud Storage staging using datadrift and its options, which are auto
    creating table, and setting new columns bigquery modes, but using AVRO as staging.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_FOR_DRIFT)
    drift_data = '\n'.join(json.dumps(rec) for rec in ROWS_FOR_DRIFT_EXT)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            staging_file_format='AVRO',
                            create_table=auto_create_table,
                            create_dataset=auto_create_dataset,
                            new_columns_mode=new_columns_mode,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    # Then, we create the second pipeline, which will try to add new columns to the first table result
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=drift_data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            staging_file_format='AVRO',
                            create_table=auto_create_table,
                            create_dataset=auto_create_dataset,
                            new_columns_mode=new_columns_mode,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline_2 = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        # auto_create_dataset needs to have auto_create_table, because you need to create tables inside the new dataset
        # that is managed by the library itself (disabling auto_create_dataset when auto_create_table is disabled),
        # but we need to create the dataset also if the auto_create_table is false
        if not auto_create_dataset or not auto_create_table:
            logger.info(f'Creating dataset {dataset_name} ...')
            bigquery_client.create_dataset(dataset_ref)

        if not auto_create_table:
            logger.info(f'Creating table {table_name} ...')
            bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA_FOR_DRIFT))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data

        # Then, we run the second pipeline
        sdc_executor.add_pipeline(pipeline_2)
        sdc_executor.start_pipeline(pipeline_2).wait_for_finished()

        # And Verify again with the new data
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        # Else, the id values are integers
        expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_NONE_COLUMN]
        expected_data += [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT_EXT]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('file_format', ['CSV', 'AVRO', 'JSON'])
def test_cdc_merge(sdc_builder, sdc_executor, gcp, file_format):
    """Test for Google BigQuery with Google Cloud Storage staging merge process.

    The pipeline looks like this:
        dev_raw_data_source >> expression_evaluator >> field_remover >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'STF_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'STF_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in CDC_ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            staging_file_format=file_format,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            merge_cdc_data=True,
                            quote_character="|",
                            column_separator=":",
                            key_columns=[{
                                "keyColumns": [
                                    "NAME"
                                ],
                                "table": table_name
                            }])

    if Version(sdc_builder.version) >= Version("5.7.0"):
        bigquery.primary_key_location = "TABLE"

    dev_raw_data_source >> expression_evaluator >> field_remover >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline has automatically created it
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in CDC_RESULT_ROWS]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@sdc_min_version('5.7.0')
def test_cdc_merge_header_pk(sdc_builder, sdc_executor, gcp):
    """We will set up the headers the same way JDBC origins do. We will have the primary keys and
    pk values updates as well.

    The pipeline looks like this:
        dev_raw_data_source >> expression_evaluator >> field_remover >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'STF_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'STF_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    rows = CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY
    for row, header in zip(rows, CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER):
        row['HEADER'] = header
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=json.dumps(rows),
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {
            'attributeToSet': 'sdc.operation.type',
            'headerAttributeExpression': "${record:value('/HEADER/sdc.operation.type')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".ID')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".ID')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".TYPE')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".TYPE')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_SPECIFICATION}',
            'headerAttributeExpression': '{\"ID\":{}, \"TYPE\":{}}'
        }
    ])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP', '/HEADER']

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            merge_cdc_data=True)

    dev_raw_data_source >> expression_evaluator >> field_remover >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline has automatically created it
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = rows[6]
        assert data_from_bigquery == [(expected_data['TYPE'], expected_data['ID'],
                                       expected_data['NAME'], expected_data['SURNAME'], expected_data['ADDRESS'])]
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


def test_basic_with_special_quoting(sdc_builder, sdc_executor, gcp):
    """Test for Google BigQuery with Google Cloud Storage staging, with uncommon quote character and column
    separator and uncommonly formatted input data.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_QUOTING)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=data, stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            quote_character="|",
                            column_separator=":")

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline has automatically created it
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE_QUOTING]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


def test_cdc_merge_with_special_quoting(sdc_builder, sdc_executor, gcp):
    """Test for Google BigQuery with Google Cloud Storage staging merge process.

    The pipeline looks like this:
        dev_raw_data_source >> expression_evaluator >> field_remover >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in CDC_ROWS_IN_DATABASE_QUOTING)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            merge_cdc_data=True,
                            quote_character="|",
                            column_separator=":",
                            key_columns=[{
                                "keyColumns": [
                                    "NAME"
                                ],
                                "table": table_name
                            }])

    if Version(sdc_builder.version) >= Version("5.7.0"):
        bigquery.primary_key_location = "TABLE"

    dev_raw_data_source >> expression_evaluator >> field_remover >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline has automatically created it
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in CDC_RESULT_ROWS_QUOTING]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('null_value', ['', 'NULL', 'StreamSets'])
def test_basic_values_as_null(sdc_builder, sdc_executor, gcp, null_value):
    """Test for Google BigQuery with Google Cloud Storage staging for null values.
    This test makes no sense for Avro, as NULL values are described in the schema, not as a null marker (it
    is not allowed in BigQuery).

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_NULL)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            null_value=null_value)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline has automatically created it
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(None if v == null_value else v for v in d.values()) for d in ROWS_IN_DATABASE_NULL]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('null_value, expected', [('', ''), (None, None), ('\\N', None), ('test', 'test')])
@sdc_min_version('5.8.0')
def test_null_values_for_json_file_format(sdc_builder, sdc_executor, gcp, null_value, expected):
    """
    Test for Google BigQuery with Google Cloud Storage staging with JSON file format for null values.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_NULL_VALUE)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            null_value=null_value,
                            staging_file_format='JSON')

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline has automatically created it
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(expected if None == v else v for v in d.values()) for d in ROWS_IN_DATABASE_NULL_VALUE]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)



@pytest.mark.parametrize('file_format', ['CSV', 'AVRO', 'JSON'])
@pytest.mark.parametrize('partition_type', ['DATE', 'DATETIME', 'TIMESTAMP', 'INTEGER', 'INGESTION'])
def test_partition_tables_types(sdc_builder, sdc_executor, gcp, partition_type, file_format):
    """Test for Google BigQuery with Google Cloud Storage staging with datadrift (as we need datadrift to create
     partitioned tables).

    The pipeline looks like this:
        dev_data_generator >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    records_count = 20

    partition = {"dataset": dataset_name,
                 "table": table_name,
                 "partitionType": partition_type,
                 "timePartitionExpiration": 0}

    if partition_type == 'INGESTION':
        # it could be whatever, we do not partition on any column here
        partition["timePartitionType"] = "MONTH"
        data_type = 'STRING'
    elif partition_type == 'TIMESTAMP':
        partition["columnName"] = "partition_column"
        partition["timePartitionType"] = "MONTH"
        data_type = 'DATETIME'
    elif partition_type in ['DATE', 'DATETIME']:
        partition["columnName"] = "partition_column"
        partition["timePartitionType"] = "MONTH"
        data_type = partition_type
    elif partition_type == 'INTEGER':
        partition["columnName"] = "partition_column"
        partition["integerPartitionStart"] = -1000
        partition["integerPartitionStep"] = 100
        partition["integerPartitionEnd"] = 1000
        data_type = partition_type

    # Build the pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev data generator
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=10,
                                      records_to_be_generated=records_count,
                                      fields_to_generate=[
                                          {"type": data_type, "field": "partition_column"},
                                          {"type": "POKEMON", "field": "name"}
                                      ])

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            staging_file_format=file_format,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            partition_table=True,
                            partition_configuration=[partition])

    dev_data_generator >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        # Assert table is partitioned as well
        if partition_type == 'INTEGER':
            assert table.range_partitioning.field == 'partition_column'
            assert table.range_partitioning.range_.start == -1000
            assert table.range_partitioning.range_.interval == 100
            assert table.range_partitioning.range_.end == 1000
        elif partition_type == 'INGESTION':
            assert table.time_partitioning.type_ == 'MONTH'
        else:
            assert table.time_partitioning.field == 'partition_column'
            assert table.time_partitioning.type_ == 'MONTH'
        # And that we have records in the table
        assert len(data_from_bigquery) == records_count
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('data_drift', [True, False])
@pytest.mark.parametrize('eval_value', ['TABLE', 'DATASET', 'BOTH'])
def test_eval_values(sdc_builder, sdc_executor, gcp, data_drift, eval_value):
    """Test for Google BigQuery with Google Cloud Storage staging with and without datadrift and eval values in table
     and dataset.

    The pipeline looks like this:
        dev_raw_data_source >> expression_evaluator >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.dataset.name',
         'headerAttributeExpression': dataset_name},
        {'attributeToSet': 'sdc.table.name',
         'headerAttributeExpression': table_name}]
    )

    # Google BigQuery destination stage
    dataset_config = "${record:attribute('sdc.dataset.name')}" if eval_value in {"DATASET", "BOTH"} else dataset_name
    table_config = "${record:attribute('sdc.table.name')}" if eval_value in {"TABLE", "BOTH"} else table_name
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_config,
                            table=table_config,
                            bucket=bucket_name,
                            enable_data_drift=data_drift,
                            create_table=data_drift,
                            create_dataset=data_drift,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> expression_evaluator >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        if not data_drift:
            logger.info('Creating dataset %s and table %s using Google BigQuery client ...', dataset_name, table_name)
            bigquery_client.create_dataset(dataset_ref)
            table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if data_drift:
            table = bigquery_client.get_table(f'{dataset_name}.{table_name}')

        # Verify by reading records using Google BigQuery client
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


def test_partition_tables_default_partition(sdc_builder, sdc_executor, gcp):
    """Test for Google BigQuery with Google Cloud Storage staging with datadrift (as we need datadrift to create
     partitioned tables) and eval values in table and dataset. We create 2 tables with datadrift, one getting
     partitioned by its config, and another one by the default one

    The pipeline looks like this:
        dev_data_generator >> expression_evaluator >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name_1 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name_1 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name_2 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name_2 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_el_var = "${record:attribute('sdc.dataset.name')}"
    table_el_var = "${record:attribute('sdc.table.name')}"
    records_count = 20

    # Build the pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev data generator
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=10,
                                      records_to_be_generated=records_count,
                                      fields_to_generate=[{"type": "POKEMON", "field": "name"},
                                                          {"type": "INTEGER", "field": "id"}])

    # Build Stream Selector
    selector = pipeline_builder.add_stage('Stream Selector')

    # Build Expression Evaluators
    expression_evaluator_1 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_1.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.dataset.name',
         'headerAttributeExpression': dataset_name_1},
        {'attributeToSet': 'sdc.table.name',
         'headerAttributeExpression': table_name_1}]
    )

    expression_evaluator_2 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_2.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.dataset.name',
         'headerAttributeExpression': dataset_name_2},
        {'attributeToSet': 'sdc.table.name',
         'headerAttributeExpression': table_name_2}]
    )

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_el_var,
                            table=table_el_var,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            partition_table=True,
                            partition_configuration=[
                                {"dataset": dataset_name_1,
                                 "table": table_name_1,
                                 "partitionType": "INGESTION",
                                 "timePartitionType": "MONTH",
                                 "timePartitionExpiration": 0},
                                {"defaultPartition": True,
                                 "partitionType": "INGESTION",
                                 "timePartitionType": "YEAR",
                                 "timePartitionExpiration": 0}
                            ])

    dev_data_generator >> selector >> expression_evaluator_1 >> bigquery
    selector >> expression_evaluator_2 >> bigquery

    selector.condition = [dict(outputLane=selector.output_lanes[0], predicate='${record:value(\'/id\')%2==0}'),
                          dict(outputLane=selector.output_lanes[1], predicate='default')]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref_1 = DatasetReference(gcp.project_id, dataset_name_1)
    dataset_ref_2 = DatasetReference(gcp.project_id, dataset_name_2)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        table_1 = bigquery_client.get_table(f'{dataset_name_1}.{table_name_1}')
        data_from_bigquery_1 = [tuple(row.values()) for row in bigquery_client.list_rows(table_1)]
        data_from_bigquery_1.sort()

        table_2 = bigquery_client.get_table(f'{dataset_name_2}.{table_name_2}')
        data_from_bigquery_2 = [tuple(row.values()) for row in bigquery_client.list_rows(table_2)]
        data_from_bigquery_2.sort()

        # Assert table is partitioned as well
        assert table_1.time_partitioning.type_ == 'MONTH'
        assert table_2.time_partitioning.type_ == 'YEAR'
        assert len(data_from_bigquery_1) + len(data_from_bigquery_2) == records_count
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref_1)
        _clean_up_bigquery(bigquery_client, dataset_ref_2)
        _clean_up_gcs(gcp, bucket, bucket_name)


def test_partition_tables_no_partition(sdc_builder, sdc_executor, gcp):
    """Test for Google BigQuery with Google Cloud Storage staging with datadrift (as we need datadrift to create
     partitioned tables) and eval values in table and dataset. Might happen that a table does not get partitioned
     because of no default partition config. We want to test the table is not partitioned

    The pipeline looks like this:
        dev_data_generator >> expression_evaluator >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_el_var = "${record:attribute('sdc.dataset.name')}"
    table_el_var = "${record:attribute('sdc.table.name')}"
    records_count = 20

    # Build the pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev data generator
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=10,
                                      records_to_be_generated=records_count,
                                      fields_to_generate=[
                                          {"type": "POKEMON", "field": "name"}
                                      ])

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.dataset.name',
         'headerAttributeExpression': dataset_name},
        {'attributeToSet': 'sdc.table.name',
         'headerAttributeExpression': table_name}]
    )

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_el_var,
                            table=table_el_var,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True,
                            partition_table=True,
                            partition_configuration=[
                                {"dataset": "wrong_dataset",
                                 "table": "wrong_table",
                                 "partitionType": "INGESTION",
                                 "timePartitionType": "MONTH",
                                 "timePartitionExpiration": 0}
                            ])

    dev_data_generator >> expression_evaluator >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        # Assert table is not partitioned
        assert not table.time_partitioning
        # And that we have records in the table
        assert len(data_from_bigquery) == records_count
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('eval_value', [True, False])
def test_multithreading(sdc_builder, sdc_executor, gcp, eval_value):
    """Test for Google BigQuery with Google Cloud Storage staging to test multithreading with and without eval values
    in dataset and tables. We also try with 2 different datasets and tables when using elvals to test splitting tables
    into different threads works as expected

    The pipeline looks like this:
    dev_data_generator >> selector >> expression_evaluator_1 >> bigquery
    selector >> expression_evaluator_2 >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name_1 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name_1 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name_2 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name_2 = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_el_var = "${record:attribute('sdc.dataset.name')}"
    table_el_var = "${record:attribute('sdc.table.name')}"
    records_count = 20

    # Build the pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev data generator
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=10,
                                      number_of_threads=2,
                                      records_to_be_generated=records_count,
                                      fields_to_generate=[{"type": "POKEMON", "field": "name"},
                                                          {"type": "INTEGER", "field": "id"}])

    # Build Stream Selector
    selector = pipeline_builder.add_stage('Stream Selector')

    # Build Expression Evaluators
    expression_evaluator_1 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_1.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.dataset.name',
         'headerAttributeExpression': dataset_name_1},
        {'attributeToSet': 'sdc.table.name',
         'headerAttributeExpression': table_name_1}]
    )

    expression_evaluator_2 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_2.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.dataset.name',
         'headerAttributeExpression': dataset_name_2},
        {'attributeToSet': 'sdc.table.name',
         'headerAttributeExpression': table_name_2}]
    )

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    dataset_config = dataset_el_var if eval_value else dataset_name_1
    table_config = table_el_var if eval_value else table_name_1

    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_config,
                            table=table_config,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True)

    dev_data_generator >> selector >> expression_evaluator_1 >> bigquery
    selector >> expression_evaluator_2 >> bigquery

    selector.condition = [dict(outputLane=selector.output_lanes[0], predicate='${record:value(\'/id\')%2==0}'),
                          dict(outputLane=selector.output_lanes[1], predicate='default')]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref_1 = DatasetReference(gcp.project_id, dataset_name_1)
    dataset_ref_2 = DatasetReference(gcp.project_id, dataset_name_2)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        table_1 = bigquery_client.get_table(f'{dataset_name_1}.{table_name_1}')
        data_from_bigquery_1 = [tuple(row.values()) for row in bigquery_client.list_rows(table_1)]
        data_from_bigquery_1.sort()

        if eval_value:
            table_2 = bigquery_client.get_table(f'{dataset_name_2}.{table_name_2}')
            data_from_bigquery_2 = [tuple(row.values()) for row in bigquery_client.list_rows(table_2)]
            data_from_bigquery_2.sort()

        if eval_value:
            assert len(data_from_bigquery_1) + len(data_from_bigquery_2) == records_count
        else:
            assert len(data_from_bigquery_1) == records_count
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref_1)
        if eval_value:
            _clean_up_bigquery(bigquery_client, dataset_ref_2)
        _clean_up_gcs(gcp, bucket, bucket_name)


@sdc_min_version('5.6.1')
@pytest.mark.parametrize('number_of_threads_and_tables', [15])
@pytest.mark.parametrize('number_of_records', [10_000])
@pytest.mark.parametrize('batch_size', [1_000])
def test_multithreaded_multiple_tables_date_types(sdc_builder, sdc_executor, gcp, number_of_threads_and_tables,
                                                  number_of_records, batch_size):
    """
        Similar to test_multithreaded_multiple_tables in Snowflake destination, ensuring multiple tables work
        for DATE/DATETIME types. The creation of these types is not always thread safe, so we need to make sure.
    """

    fields_to_generate = [{'type': 'LONG_SEQUENCE', 'field': 'ID'},
                          {'type': 'DATE', 'field': 'DATE'},
                          {'type': 'DATETIME', 'field': 'DATETIME'},
                          {'type': 'TIME', 'field': 'TIME'}]

    def convert(field, field_type):
        if field_type == 'LONG_SEQUENCE':
            field = str(field)
        elif field_type == 'DATE':
            field = field.date()
        elif field_type == 'TIME':
            field = field.time()
        return field

    # We generate the number of tables we need
    random_table_suffix = get_random_string(ascii_lowercase, 5)
    table_names = [f'STF_TABLE_{idx}_{random_table_suffix}' for idx in range(number_of_threads_and_tables)]
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        records_to_be_generated=number_of_records, batch_size=batch_size,
        number_of_threads=number_of_threads_and_tables, delay_between_batches=10,
        fields_to_generate=fields_to_generate
    )

    # We need to deduplicate records as dev_generator might create the same record in multiple threads
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    record_deduplicator.set_attributes(compare="SPECIFIED_FIELDS", fields_to_compare=['/ID'])
    trash = pipeline_builder.add_stage('Trash')

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table="STF_TABLE_${record:value('/ID') % "
                                  + str(number_of_threads_and_tables)
                                  + '}_' + random_table_suffix,
                            bucket=bucket_name,
                            enable_data_drift=True,
                            create_table=True,
                            purge_stage_file_after_ingesting=True)

    if Version(sdc_builder.version) < Version("5.7.0"):
        bigquery.set_attributes(connection_pool_size=number_of_threads_and_tables)
    else:
        bigquery.set_attributes(maximum_connection_threads=number_of_threads_and_tables)

    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> record_deduplicator >> [bigquery, wiretap.destination]
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)
        bigquery_client.create_dataset(dataset_ref)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=1800)

        # We collect the data received in wiretap and create an array for each table
        expected_data_per_table = {table_name: [] for table_name in table_names}
        for record in wiretap.output_records:
            record_table_name = \
                f'STF_TABLE_{int(str(record.field["ID"])) % number_of_threads_and_tables}_{random_table_suffix}'
            expected_record = ()
            for field in fields_to_generate:
                expected_record = expected_record + (convert(record.field[field['field']].value, field['type']),)
            expected_data_per_table[record_table_name].append(expected_record)

        # And then for each table, we check what we received in wiretap matches
        for table_name in table_names:
            # Order by id per table to compare
            table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
            rows = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
            assert len(rows) == len(expected_data_per_table[table_name])
            sorted_data_from_database = sorted(rows, key=lambda x: x[0])
            sorted_expected_data = sorted(expected_data_per_table[table_name], key=lambda x: x[0])
            # And compare sorted data per table
            assert sorted_data_from_database == sorted_expected_data, \
                'Data read from BigQuery should have been the same as the data captured in wiretap.'

    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


def test_allow_quoted_newlines(sdc_builder, sdc_executor, gcp):
    """Test for Google BigQuery with Google Cloud Storage staging. Purpose of the test is the Allow Quoted Newlines
    config, with a quoted newline character in the inserted data.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_QUOTED_NEWLINES)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            enable_data_drift=False,
                            create_table=False,
                            purge_stage_file_after_ingesting=True,
                            allow_quoted_newlines=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s and table %s using Google BigQuery client ...', dataset_name, table_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE_QUOTED_NEWLINES]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('file_format', ['AVRO', 'JSON'])
@pytest.mark.parametrize('bigquery_create_schema', [True, False])
def test_nested_fields_map(sdc_builder, sdc_executor, gcp, bigquery_create_schema, file_format):
    """Test for Google BigQuery with Google Cloud Storage staging testing nested fields.

    CSV and nested fields is not supported by BigQuery. So for now, we can just test it using Avro and Json

    IMPORTANT: Sadly, if using AVRO, BigQuery converts map fields to a repeated RECORD that contains two
    fields: key and value, instead of the actual map. So we have to check the schema in the same way.
    https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#complex_types

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in NESTED_MAP_TO_LOAD)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='BIGQUERY' if bigquery_create_schema else 'SDC',
                            staging_file_format=file_format,
                            enable_data_drift=not bigquery_create_schema,
                            create_table=not bigquery_create_schema,
                            create_dataset=False,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        if file_format == 'AVRO':
            expected_data = [tuple(v for v in d.values()) for d in EXPECTED_AVRO_MAP_IN_DATABASE]
        elif file_format == 'JSON':
            expected_data = [tuple(v for v in d.values()) for d in NESTED_MAP_TO_LOAD]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('file_format', ['AVRO', 'JSON'])
@pytest.mark.parametrize('bigquery_create_schema', [True, False])
def test_nested_fields_list(sdc_builder, sdc_executor, gcp, bigquery_create_schema, file_format):
    """Test for Google BigQuery with Google Cloud Storage staging testing nested fields.

    CSV and nested fields is not supported by BigQuery. So for now, we can just test it using Avro and Json

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in NESTED_LIST_TO_LOAD)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='BIGQUERY' if bigquery_create_schema else 'SDC',
                            staging_file_format=file_format,
                            enable_data_drift=not bigquery_create_schema,
                            create_table=not bigquery_create_schema,
                            create_dataset=False,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in NESTED_LIST_TO_LOAD]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('input_data', [JSON_NESTED_MAP_TO_LOAD,
                                        JSON_NESTED_LIST_OF_MAPS_TO_LOAD,
                                        JSON_NESTED_LIST_OF_LISTS_OF_MAPS_TO_LOAD])
@pytest.mark.parametrize('bigquery_create_schema', [True, False])
def test_json_nested_fields(sdc_builder, sdc_executor, gcp, bigquery_create_schema, input_data):
    """Test for Google BigQuery with Google Cloud Storage staging testing nested fields.

    JSON allows some more complicated nested structures that include different types for its subfields.

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in input_data)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='BIGQUERY' if bigquery_create_schema else 'SDC',
                            staging_file_format='JSON',
                            enable_data_drift=not bigquery_create_schema,
                            create_table=not bigquery_create_schema,
                            create_dataset=False,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in input_data]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('bigquery_create_schema', [True, False])
def test_json_nested_fields_with_nulls(sdc_builder, sdc_executor, gcp, bigquery_create_schema):
    """Test for Google BigQuery with Google Cloud Storage staging testing nested fields.

    Similar to test_json_nested_fields, but with special case handling NULLS (empty) inside nested structures

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in JSON_NESTED_LIST_OF_LISTS_OF_MAPS_WITH_NULLS_TO_LOAD)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='BIGQUERY' if bigquery_create_schema else 'SDC',
                            staging_file_format='JSON',
                            enable_data_drift=not bigquery_create_schema,
                            create_table=not bigquery_create_schema,
                            create_dataset=False,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in EXPECTED_JSON_NESTED_LIST_OF_LISTS_OF_MAPS_WITH_NULLS]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@pytest.mark.parametrize('file_format', ['AVRO', 'JSON'])
def test_nested_fields_data_drift(sdc_builder, sdc_executor, gcp, file_format):
    """Test for Google BigQuery with Google Cloud Storage staging testing nested fields when the schema drifts.
    BigQuery does not allow drift in schema, so just testing for SDC schema generation.

    CSV and nested fields is not supported by BigQuery. So for now, we can just test it using Avro and Json

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """

    if Version(sdc_builder.version) < Version('5.5.0') and file_format == 'JSON':
        pytest.skip('JSON staging introduced in 5.5.0')

    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in NESTED_LIST_TO_LOAD)
    drift_data = '\n'.join(json.dumps(rec) for rec in NESTED_DRIFT_TO_LOAD)

    # First, we create the initial pipeline, which will create datasets and tables as needed
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='SDC',
                            staging_file_format=file_format,
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    # Then, we create the second pipeline, which will try to add new columns to the first table result
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=drift_data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            schema_generator='SDC',
                            staging_file_format='AVRO',
                            enable_data_drift=True,
                            create_table=True,
                            create_dataset=True,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> bigquery

    pipeline_2 = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)

    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify by reading records using Google BigQuery client
        # We retrieve the table after the pipeline is run
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in NESTED_LIST_TO_LOAD]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data

        # Then, we run the second pipeline
        sdc_executor.add_pipeline(pipeline_2)
        sdc_executor.start_pipeline(pipeline_2).wait_for_finished()

        # And Verify again with the new data
        table = bigquery_client.get_table(f'{dataset_name}.{table_name}')
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(v for v in d.values()) for d in EXPECTED_NESTED_LIST_WITH_DRIFT]

        assert len(data_from_bigquery) == len(expected_data)
        assert data_from_bigquery == expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


def start_pipeline_and_check_stopped(sdc_executor, pipeline, wiretap):
    with pytest.raises(Exception):
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.stop_pipeline()
    response = sdc_executor.get_pipeline_status(pipeline).response.json()
    status = response.get('status')
    logger.info('Pipeline status %s ...', status)
    assert 'RUN_ERROR' == status, response


def start_pipeline_and_check_to_error(sdc_executor, pipeline, wiretap):
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert 2 == len(wiretap.error_records)


@sdc_min_version('5.4.0')
@pytest.mark.parametrize("on_error_record, start_and_check",
                         [("STOP_PIPELINE", start_pipeline_and_check_stopped),
                          ("TO_ERROR", start_pipeline_and_check_to_error)])
def test_gcp_write_records_on_error(sdc_builder, sdc_executor, gcp,
                                    on_error_record, start_and_check):
    """
    Write DB with malformed records and check pipeline behaves as set in 'on_record_error'

    The pipeline looks like this:
        dev_raw_data_source >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_WITH_ERROR)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=data,
                                       stop_after_first_batch=True)

    # Google BigQuery destination stage
    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            staging_file_format='CSV',
                            enable_data_drift=False,
                            create_table=False,
                            on_record_error=on_error_record,
                            purge_stage_file_after_ingesting=True)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [bigquery, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)
    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)
        logger.info('Creating dataset %s and table %s using Google BigQuery client ...', dataset_name, table_name)
        bigquery_client.create_dataset(dataset_ref)
        bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        start_and_check(sdc_executor, pipeline, wiretap)
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)


@sdc_min_version('5.7.0')
@pytest.mark.parametrize('file_format', ['CSV', 'JSON'])  # AVRO and data types handling is special
@pytest.mark.parametrize('use_defaults', [True, False])
@pytest.mark.parametrize('ignore_missing_fields', [True, False])
def test_missing_field(sdc_builder, sdc_executor, gcp, file_format, use_defaults, ignore_missing_fields):
    """
    Check that missing field and invalid types work as expected, using Data Advanced default values.
    Similar to standard-test_data_type

    The setup might look complicated, but it's just because each data type is used with its own default,
    coming from strings

    The pipeline looks like:
    dev_raw_data_source >> field_type_converter >> field_replacer >> bigquery
    """
    bucket_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    header_to_insert = ""
    rows_to_insert = ""
    data_to_expect = []
    converter_configs = []
    schema = []
    replacement_rules = []
    # we create a field for each data type, to later on set it to null (to test missing field with each type)
    # but the table is created with other fields, so all fields are effectively missing
    for data_type in DATA_TYPES.keys():
        index = list(DATA_TYPES).index(data_type)
        field_name = 'data_' + data_type

        # fill stages based on the different data types and file formats
        if data_type == 'STRUCT':
            if file_format == 'CSV':
                # CSV won't support nested types even with null values
                continue
            # struct is a bit different, as we cannot default to anything without knowing the schema
            schema.insert(index, SchemaField(field_name, data_type, fields=(SchemaField('data', 'STRING'),)))
        elif data_type == 'GEOGRAPHY':
            # geography is also not supported as default, same reasoning as struct, hard to get from string
            schema.insert(index, SchemaField(field_name, data_type))
        else:
            schema.insert(index, SchemaField(field_name, data_type))
            missing_field_name = 'missing_data_' + data_type
            header_to_insert = header_to_insert + missing_field_name + ","
            # rows to insert is actually never inserted, as it's set to null later on
            # to make sure, but we use same value for simplicity of getting a valid value
            rows_to_insert = rows_to_insert + str(DATA_TYPES.get(data_type)[1]) + ","

            if data_type == 'DATE':
                date_format = 'YYYY_MM_DD'
                other_date_format = ''
            elif data_type == 'TIME':
                date_format = 'OTHER'
                other_date_format = 'HH:MM:SS'
            else:
                date_format = 'YYYY_MM_DD_HH_MM_SS'
                other_date_format = ''

            converter_configs.insert(index, {
                'fields': ['/' + missing_field_name],
                'targetType': DATA_TYPES.get(data_type)[0],
                'dataLocale': 'en,US',
                'dateFormat': date_format,
                'otherDateFormat': other_date_format,
                'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
                'scale': 38
            })
            replacement_rules.insert(index, {"fields": '/' + missing_field_name, "setToNull": True})

        # fill expected data
        if use_defaults:
            data_to_expect.insert(index, DATA_TYPES.get(data_type)[2])
        else:
            if file_format == 'JSON' and data_type == 'BYTES':
                # bytes in json in an empty bytes string, not none
                data_to_expect.insert(index, b'')
            elif file_format == 'JSON' and data_type == 'STRING':
                # string in json in an empty string, not none
                data_to_expect.insert(index, '')
            else:
                data_to_expect.insert(index, None)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       stop_after_first_batch=True,
                                       raw_data=header_to_insert[:-1] + '\n' + rows_to_insert[:-1])

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.conversion_method = 'BY_FIELD'
    field_type_converter.field_type_converter_configs = converter_configs

    field_replacer = pipeline_builder.add_stage("Field Replacer")
    field_replacer.set_attributes(replacement_rules=replacement_rules)

    defaults = {
        'numeric_types_default': str(DATA_TYPES.get('INTEGER')[1]),
        'float_default': str(DATA_TYPES.get('FLOAT')[1]),
        'boolean_default': str(DATA_TYPES.get('BOOLEAN')[1]),
        # struct has no default option, just empty
        # geography has no default option, just empty
        'bytes_default': str(DATA_TYPES.get('BYTES')[1]),
        'decimal_default': str(DATA_TYPES.get('DECIMAL')[1]),
        'date_default': str(DATA_TYPES.get('DATE')[1]),
        'datetime_default': str(DATA_TYPES.get('DATETIME')[1]),
        'time_default': str(DATA_TYPES.get('TIME')[1]),
        'timestamp_default': str(DATA_TYPES.get('TIMESTAMP')[1]),
        'string_default': str(DATA_TYPES.get('STRING')[1])
    } if use_defaults else {}

    bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    bigquery.set_attributes(project_id=gcp.project_id,
                            dataset=dataset_name,
                            table=table_name,
                            bucket=bucket_name,
                            staging_file_format=file_format,
                            enable_data_drift=False,
                            create_table=False,
                            ignore_missing_fields=ignore_missing_fields,
                            # these defaults were added in 5.6, but if sdc<5.6 and no defaults are specified,
                            # we get an ugly exception as some defaults were not handled properly
                            **defaults,
                            purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> field_type_converter >> field_replacer >> bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)
    try:
        logger.info(f'Creating temporary bucket {bucket_name}')
        bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)

        logger.info('Creating dataset %s and table %s using Google BigQuery client ...', dataset_name, table_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        expected_data = [tuple(data_to_expect)]
        expected_data.sort()

        history = sdc_executor.get_pipeline_history(pipeline)
        if ignore_missing_fields:
            if file_format == 'CSV' and Version(sdc_builder.version) >= Version('5.8.0'):
                assert history.latest.metrics.counter('stage.GoogleBigQuery_01.errorRecords.counter').count == 1
                assert len(data_from_bigquery) == 0
            else:
                assert history.latest.metrics.counter('stage.GoogleBigQuery_01.errorRecords.counter').count == 0
                assert len(data_from_bigquery) == len(expected_data)
                assert data_from_bigquery == expected_data
        else:
            assert history.latest.metrics.counter('stage.GoogleBigQuery_01.errorRecords.counter').count == 1
            assert len(data_from_bigquery) == 0
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)
        _clean_up_gcs(gcp, bucket, bucket_name)