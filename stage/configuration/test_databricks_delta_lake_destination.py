#  Copyright (c) 2023 StreamSets Inc.

import json
import logging
import pytest
import re
from .. import _clean_up_databricks

from streamsets.sdk.utils import Version
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import aws, azure, deltalake, sdc_min_version
from streamsets.testframework.utils import get_random_string

DESTINATION_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_DatabricksDeltaLakeDTarget'
MIN_VERSION_UNITY_CATALOG = 11
REGEX_EXPRESSION_DATABRICKS_VERSION = re.compile(r'\d+')

pytestmark = [deltalake, sdc_min_version('5.5.0')]

logger = logging.getLogger(__name__)

PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'
PRIMARY_KEY_SPECIFICATION = 'jdbc.primaryKeySpecification'

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
    {"TITLE": "Europe, Through the Back Door",
     "AUTHOR": "Rick Steves",
     "GENRE": "Travel",
     "PUBLISHER": "Rick Steves"},
    {"foo": "bar"},
    {"foo": "bar"},
    {"title": "Steve Jobs",
     "author": "Walter Isaacson",
     "genre": "Biography",
     "publisher": "Simon & Schuster"},
    {"title": "The Spy and the Traitor: The Greatest Espionage Story of the Cold War",
     "author": "Ben Macintyre",
     "genre": "Biography True crime",
     "publisher": "McClelland & Stewart"}
]

ROWS_IN_DATABASE_WITH_NEWLINE = [
    {"title": "Elon Musk: Tesla SpaceX and\nthe Quest for a Fantastic Future",
     "author": "Ashlee Vance",
     "genre": "Biography",
     "publisher": "HarperCollins Publishers"}
]

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


def set_sdc_stage_config(deltalake, config, value):
    # There is this stf issue that sets up 2 configs are named the same, both configs are set up
    # If the config is an enum, it created invalid pipelines (e.g. Authentication Method in azure and s3 staging)
    # This acts as a workaround to only set that specific config
    deltalake.sdc_stage_configurations[DESTINATION_STAGE_NAME][config] = value


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_access_key_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'ADLS_GEN2'}])
def test_account_fqdn(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'azure_authentication_method': 'SHARED_KEY',
                                               'staging_location': 'ADLS_GEN2'}])
def test_account_shared_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes',
                         [{'azure_authentication_method': 'OAUTH', 'staging_location': 'ADLS_GEN2'}])
def test_application_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes',
                         [{'azure_authentication_method': 'OAUTH', 'staging_location': 'ADLS_GEN2'}])
def test_application_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes',
                         [{'azure_authentication_method': 'OAUTH', 'staging_location': 'ADLS_GEN2'}])
def test_auth_token_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'auto_create_table': False, 'enable_data_drift': True},
                                              {'auto_create_table': True, 'enable_data_drift': True}])
def test_auto_create_table(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'azure_authentication_method': 'OAUTH', 'staging_location': 'ADLS_GEN2'},
                                              {'azure_authentication_method': 'SHARED_KEY',
                                               'staging_location': 'ADLS_GEN2'}])
def test_azure_authentication_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_binary_default(sdc_builder, sdc_executor):
    pass


@stub
def test_boolean_default(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_bucket(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_column_fields_to_ignore(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_pool_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_new_columns_as_string': False, 'enable_data_drift': True},
                                              {'create_new_columns_as_string': True, 'enable_data_drift': True}])
def test_create_new_columns_as_string(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_date_default(sdc_builder, sdc_executor):
    pass


@stub
def test_decimal_default(sdc_builder, sdc_executor):
    pass


@aws('s3')
def test_table_location_path(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.
    Verifies that the property for table location respects where the table is created

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'
    table_location = '/deltalake/'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True)

    if Version(sdc_builder.version) < Version("5.7.0"):
        databricks_deltalake.directory_for_table_location = table_location
    else:
        databricks_deltalake.table_location_path = table_location

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=180)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]

        result = connection.execute(f'Show create table {table_name}')

        assert f'dbfs:{table_location}{table_name}' in str(result.fetchall()[0])
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@stub
def test_double_default(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_data_drift': False}, {'enable_data_drift': True}])
def test_enable_data_drift(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_float_default(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_fields_with_invalid_types': False},
                                              {'ignore_fields_with_invalid_types': True}])
def test_ignore_fields_with_invalid_types(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_fields': False}, {'ignore_missing_fields': True}])
def test_ignore_missing_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_jdbc_url(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'merge_cdc_data': True}])
def test_key_columns(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'merge_cdc_data': False}, {'merge_cdc_data': True}])
def test_merge_cdc_data(sdc_builder, sdc_executor, stage_attributes):
    pass


@aws('s3')
def test_cdc_jdbc_header(sdc_builder, sdc_executor, deltalake, aws):
    """ We will set up the headers the same way JDBC origins do. We will use primary_key_location="HEADER"

    The pipeline looks like:
    Databricks deltalake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> databricks_deltalake
    """

    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
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
    field_remover.fields = ['/HEADER']

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location="AWS_S3",
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        merge_cdc_data=True,
                                        primary_key_location="HEADER")

    dev_raw_data_source >> expression_evaluator >> field_remover >> databricks_deltalake
    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        expected_data = rows[6]
        assert data_from_database == [(expected_data['TYPE'], expected_data['ID'],
                                       expected_data['NAME'], expected_data['SURNAME'], expected_data['ADDRESS'])]

    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('newline_character', [" ", "|"])
def test_new_line_replacement_character(sdc_builder, sdc_executor, deltalake, aws, newline_character):
    """Test for Databricks Delta Lake with AWS S3 storage.
    Verify the functionality under replace new lines configuration

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """

    output_data = [{"title": f'Elon Musk: Tesla SpaceX and{newline_character}the Quest for a Fantastic Future',
                    "author": "Ashlee Vance",
                    "genre": "Biography",
                    "publisher": "HarperCollins Publishers"}]
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_WITH_NEWLINE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        replace_newlines=True,
                                        new_line_replacement_character=newline_character)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in output_data]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [tuple(record.values()) for record in data_from_database]
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('null_value', ['\\N', 'stf', ''])
def test_null_value(sdc_builder, sdc_executor, deltalake, aws, null_value):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_test_null_value_{get_random_string()}'

    rows_data = [{"title": f'{null_value}', "author": "Pepito", "genre": "Comedy",
                  "publisher": "HarperCollins Publishers"}, ]

    engine = deltalake.engine
    data = '\n'.join(json.dumps(rec) for rec in rows_data)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=data, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        null_value=null_value,
                                        purge_stage_file_after_ingesting=True,
                                        column_separator=';')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [dict(d) for d in rows_data]

        assert len(data_from_database) == len(expected_data)
        assert data_from_database[0]['title'] is None

        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0

    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('null_value', ['\\N', 'stf', ''])
def test_cdc_null_value(sdc_builder, sdc_executor, deltalake, aws, null_value):
    """Test for Databricks Delta lake with AWS S3 storage. Using CDC data as input

    The pipeline looks like this:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> databricks_deltalake
    """
    table_name = f'stf_test_cdc_null_value_{get_random_string()}'

    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()

    cdc_rows = [
        {'OP': 4, 'ID': 1, 'NAME': f'{null_value}'},
        {'OP': 4, 'ID': 2, 'NAME': 'Enric Potter'},
        {'OP': 4, 'ID': 3, 'NAME': 'Tomas Riddle'}
    ]

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in cdc_rows))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}
    ])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    table_key_columns = [{"keyColumns": ["ID"], "table": table_name}]

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location="AWS_S3",
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        null_value=null_value,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        merge_cdc_data=True,
                                        primary_key_location="TABLE",
                                        table_key_columns=table_key_columns)

    dev_raw_data_source >> expression_evaluator >> field_remover >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select ID, NAME from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in cdc_rows]

        assert len(data_from_database) == len(expected_data)
        assert data_from_database[0]['NAME'] is None

        result.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)

@aws('s3')
@sdc_min_version('5.7.0')
def test_unity_catalog(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    target_catalog = deltalake.workspace_catalog_name
    target_schema = deltalake.workspace_schema_name
    table_name = f'{target_catalog}.{target_schema}.stf_unity_catalog_{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    rows_data = [{"title": f'Bicycles are for the summer', "author": "Pepito", "genre": "Comedy",
                  "publisher": "HarperCollins Publishers"}, ]

    engine = deltalake.engine
    data = '\n'.join(json.dumps(rec) for rec in rows_data)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=data, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        auto_create_table=True,
                                        table_location_path=uri_external_location,
                                        purge_stage_file_after_ingesting=True,
                                        column_separator=';')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        assert len(data_from_database) == len(rows_data)
        assert data_from_database == [(row['title'], row['author'], row['genre'], row['publisher']) for row in
                                      rows_data]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0

    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@sdc_min_version('5.7.0')
def test_cdc_with_unity_catalog(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta lake with AWS S3 storage. Using CDC data as input

    The pipeline looks like this:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    target_catalog = deltalake.workspace_catalog_name
    target_schema = deltalake.workspace_schema_name
    table_name = f'{target_catalog}.{target_schema}.stf_cdc_unity_catalog{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()

    cdc_rows = [
        {'OP': 4, 'ID': 1, 'NAME': 'Ronaldo Weasley'},
        {'OP': 4, 'ID': 2, 'NAME': 'Enric Potter'},
        {'OP': 4, 'ID': 3, 'NAME': 'Tomas Riddle'}
    ]

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in cdc_rows))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}
    ])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # ADLS gen2 storage destination
    files_prefix = f'stf-deltalake/{get_random_string()}'

    table_key_columns = [{"keyColumns": ["ID"], "table": table_name}]

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key)

    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        merge_cdc_data=True,
                                        table_location_path=uri_external_location,
                                        primary_key_location="TABLE",
                                        table_key_columns=table_key_columns)

    set_sdc_stage_config(deltalake, 'config.dataLakeGen2Stage.connection.authMethod', 'SHARED_KEY')

    dev_raw_data_source >> expression_evaluator >> field_remover >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select ID, NAME from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in cdc_rows]

        assert len(data_from_database) == len(expected_data)
        assert data_from_database == [(row['ID'], row['NAME']) for row in cdc_rows]

        result.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@stub
def test_numeric_types_default(sdc_builder, sdc_executor):
    pass


def _start_pipeline_and_check_stopped(sdc_executor, pipeline, wiretap):
    with pytest.raises(Exception):
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.stop_pipeline()
    response = sdc_executor.get_pipeline_status(pipeline).response.json()
    status = response.get('status')
    logger.info(f'Pipeline status {status}...')
    assert status in ['RUN_ERROR', 'RUNNING_ERROR'], response


def _start_pipeline_and_check_to_error(sdc_executor, pipeline, wiretap):
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert 6 == len(wiretap.error_records), f'Error records: {wiretap.error_records}'


def _start_pipeline_and_check_discard(sdc_executor, pipeline, wiretap):
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert 0 == len(wiretap.error_records), f'Error records: {wiretap.error_records}'


@aws('s3')
@pytest.mark.parametrize("on_record_error, start_and_check",
                         [("STOP_PIPELINE", _start_pipeline_and_check_stopped),
                          ("TO_ERROR", _start_pipeline_and_check_to_error),
                          ("DISCARD", _start_pipeline_and_check_discard)])
def test_on_record_error(sdc_builder, sdc_executor, deltalake, aws,
                         on_record_error, start_and_check):
    """Write DB with malformed records and check pipeline behaves as set in 'on_record_error'

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_WITH_ERROR)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        on_record_error=on_record_error)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [databricks_deltalake, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        start_and_check(sdc_executor, pipeline, wiretap)
    finally:
        _clean_up_databricks(deltalake, table_name)


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@aws('s3')
@pytest.mark.parametrize('purge_stage_file_after_ingesting', [True, False])
def test_purge_stage_file_after_ingesting(sdc_builder, sdc_executor, aws, deltalake, purge_stage_file_after_ingesting):
    """Test for Databricks Delta Lake with AWS S3 storage. We verify that purge stage file after ingestion works as
    expected

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    # Set it after configure for environment to avoid being overridden by testframework
    databricks_deltalake.set_attributes(purge_stage_file_after_ingesting=purge_stage_file_after_ingesting)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        if purge_stage_file_after_ingesting:
            assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
        else:
            assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 1
    finally:
        try:
            if not purge_stage_file_after_ingesting:
                logger.info(f"Purging Bucket={aws.s3_bucket_name} Prefix={s3_key}")
                aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        except Exception as ex:
            logger.error(f"Error encountered while purging Bucket={aws.s3_bucket_name} Prefix={s3_key}")
            logger.error(f"As Exception {ex}")

        _clean_up_databricks(deltalake, table_name)


@stub
@pytest.mark.parametrize('stage_attributes', [{'replace_newlines': False}, {'replace_newlines': True}])
def test_replace_newlines(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_row_field(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_s3_connection_timeout(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_encryption': 'KMS', 'staging_location': 'AWS_S3'},
                                              {'s3_encryption': 'NONE', 'staging_location': 'AWS_S3'},
                                              {'s3_encryption': 'S3', 'staging_location': 'AWS_S3'}])
def test_s3_encryption(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_encryption': 'KMS', 'staging_location': 'AWS_S3'}])
def test_s3_encryption_context(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_encryption': 'KMS', 'staging_location': 'AWS_S3'}])
def test_s3_encryption_kms_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_s3_max_error_retry(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_s3_minimum_upload_part_size_in_mb(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_s3_multipart_upload_threshold_in_mb(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_proxy_authentication_enabled': False,
                                               's3_proxy_enabled': True,
                                               'staging_location': 'AWS_S3'},
                                              {'s3_proxy_authentication_enabled': True,
                                               's3_proxy_enabled': True,
                                               'staging_location': 'AWS_S3'}])
def test_s3_proxy_authentication_enabled(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_proxy_enabled': False, 'staging_location': 'AWS_S3'},
                                              {'s3_proxy_enabled': True, 'staging_location': 'AWS_S3'}])
def test_s3_proxy_enabled(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_proxy_enabled': True, 'staging_location': 'AWS_S3'}])
def test_s3_proxy_host(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_proxy_authentication_enabled': True,
                                               's3_proxy_enabled': True,
                                               'staging_location': 'AWS_S3'}])
def test_s3_proxy_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_proxy_enabled': True, 'staging_location': 'AWS_S3'}])
def test_s3_proxy_port(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'s3_proxy_authentication_enabled': True,
                                               's3_proxy_enabled': True,
                                               'staging_location': 'AWS_S3'}])
def test_s3_proxy_user(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_s3_socket_timeout(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_s3_uploading_threads(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}])
def test_secret_access_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'AWS_S3'}, {'staging_location': 'ADLS_GEN2'}])
def test_stage_file_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'ADLS_GEN2'}, {'staging_location': 'AWS_S3'}])
def test_staging_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'staging_location': 'ADLS_GEN2'}])
def test_storage_container_file_system(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_string_default(sdc_builder, sdc_executor):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
def test_timestamp_default(sdc_builder, sdc_executor):
    pass


@stub
def test_token(sdc_builder, sdc_executor):
    pass


@aws('s3')
@pytest.mark.parametrize('is_trim_spaces', [True, False])
def test_trim_spaces(sdc_builder, sdc_executor, deltalake, aws, is_trim_spaces):
    """Test for Databricks Delta Lake with different trim_spaces.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    initial_csv = [
        {'title': 'Books Title ', 'author': ' Book Author', 'genre': 'Book Genre', 'publisher': 'Book Publisher'}]

    if is_trim_spaces:
        expected_csv = [
            {'title': 'Books Title', 'author': 'Book Author', 'genre': 'Book Genre', 'publisher': 'Book Publisher'}]
    else:
        expected_csv = [
            {'title': 'Books Title ', 'author': ' Book Author', 'genre': 'Book Genre', 'publisher': 'Book Publisher'}]

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in initial_csv)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        trim_spaces=is_trim_spaces)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in expected_csv]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('use_instance_profile', [True, False])
def test_use_iam_roles(sdc_builder, sdc_executor, deltalake, aws, use_instance_profile):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> databricks_deltalake

    # In case of IAM Roles we set it to True and set keys to blank
    if use_instance_profile:
        if Version(sdc_builder.version) < Version("5.7.0"):
            databricks_deltalake.set_attributes(use_instance_profile=True, access_key_id="", secret_access_key="")
        else:
            set_sdc_stage_config(deltalake, 'config.s3Stage.connection.awsConfig.credentialMode', 'WITH_IAM_ROLES')
            set_sdc_stage_config(deltalake, 'config.s3Stage.connection.awsConfig.awsAccessKeyId', '')
            set_sdc_stage_config(deltalake, 'config.s3Stage.connection.awsConfig.awsSecretAccessKey', '')

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('quoting_mode', ['QUOTED', 'ESCAPED', 'BOTH'])
def test_quoting_mode(sdc_builder, sdc_executor, deltalake, aws, quoting_mode):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        quoting_mode=quoting_mode,
                                        column_separator=";")

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('quote_character', ['"', '|', '$'])
def test_quote_character(sdc_builder, sdc_executor, deltalake, aws, quote_character):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        quote_character=quote_character)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('column_separator', [',', ';', '|', '$', '='])
def test_column_separator(sdc_builder, sdc_executor, deltalake, aws, column_separator):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        column_separator=column_separator)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('escape_character', ['\\', '|', '(', ')', '='])
def test_escape_character(sdc_builder, sdc_executor, deltalake, aws, escape_character):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        quoting_mode='ESCAPED',
                                        escape_character=escape_character,
                                        column_separator=';')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
    finally:
        _clean_up_databricks(deltalake, table_name)
