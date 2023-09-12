#  Copyright (c) 2023 StreamSets Inc.

import json
import logging
import pytest
import string
from .. import _clean_up_databricks
from streamsets.sdk.utils import get_random_string, Version
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import aws, azure, category, deltalake, sdc_min_version

pytestmark = [deltalake, sdc_min_version('5.5.0')]
logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {"title": "Elon Musk: Tesla, SpaceX, and the Quest for a Fantastic Future",
     "author": "Ashlee Vance",
     "genre": "Biography",
     "publisher": "HarperCollins Publishers"},
    {"title": "Europe Through the Back Door",
     "author": "Rick Steves",
     "genre": "Travel",
     "publisher": "Rick Steves"},
    {"title": "Steve Jobs",
     "author": "Walter Isaacson",
     "genre": "Biography",
     "publisher": "Simon & Schuster"},
    {"title": "The Spy and the Traitor: The Greatest Espionage Story of the Cold War",
     "author": "Ben Macintyre",
     "genre": "Biography, True crime",
     "publisher": "McClelland & Stewart"}
]

# Sandbox prefix for S3 bucket
S3_COMMON_PREFIX = 'deltalake'

ADLS_GEN2_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_datalake_gen2_DataLakeGen2DTarget'
EXECUTOR_STAGE_NAME = 'com_streamsets_pipeline_stage_executor_DatabricksQueryDExecutor'
AWS_STORAGE_LOCATION = 'AWS_S3'
AZURE_STORAGE_LOCATION = 'ADLS_GEN2'
DEFAULT_STORAGE_LOCATION = 'NONE'
ADLS_GEN2_DEFAULT_AUTH_METHOD = 'SHARED_KEY'
ADLS_GEN2_CLIENT_AUTH_METHOD = 'CLIENT'
ADLS_GEN2_AUTH_METHOD_CONFIG = 'config.adlsGen2Connection.authMethod'

AZURE_AUTH_METHOD_CONFIG = {
    # OAUTH got renamed to CLIENT
    ADLS_GEN2_CLIENT_AUTH_METHOD: 'OAUTH',
    ADLS_GEN2_DEFAULT_AUTH_METHOD: 'SHARED_KEY',
}


def get_auth_method_config(sdc_builder, config):
    if Version(sdc_builder.version) < Version("5.7.0"):
        legacy_config = AZURE_AUTH_METHOD_CONFIG.get(config)
        return legacy_config if legacy_config else config
    else:
        return config


def set_sdc_stage_config(deltalake, config, value):
    # There is this stf issue that sets up 2 configs are named the same, both configs are set up
    # If the config is an enum, it created invalid pipelines (e.g. Authentication Method in azure and s3 staging)
    # This acts as a workaround to only set that specific config
    deltalake.sdc_stage_configurations[EXECUTOR_STAGE_NAME][config] = value


@category('basic')
@azure('datalake')
@pytest.mark.parametrize('authentication_method', ['SHARED_KEY'])
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'}])
def test_account_fqdn(sdc_builder, sdc_executor, deltalake, azure, authentication_method, stage_attributes):
    authentication_method = get_auth_method_config(sdc_builder, authentication_method)
    if Version(sdc_builder.version) < Version("5.7.0"):
        stage_attributes['azure_authentication_method'] = authentication_method
    else:
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, authentication_method)

    _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)


@category('basic')
@azure('datalake')
@pytest.mark.parametrize('authentication_method', ['SHARED_KEY'])
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'}])
def test_account_shared_key(sdc_builder, sdc_executor, deltalake, azure, authentication_method, stage_attributes):
    authentication_method = get_auth_method_config(sdc_builder, authentication_method)
    if Version(sdc_builder.version) < Version("5.7.0"):
        stage_attributes['azure_authentication_method'] = authentication_method
    else:
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, authentication_method)
        stage_attributes['endpoint_type'] = 'URL'

    _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)


@stub
@category('advanced')
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@category('basic')
@azure('datalake')
@pytest.mark.parametrize('authentication_method', ['CLIENT'])
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'}])
def test_application_id(sdc_builder, sdc_executor, deltalake, azure, authentication_method, stage_attributes):
    authentication_method = get_auth_method_config(sdc_builder, authentication_method)
    if Version(sdc_builder.version) < Version("5.7.0"):
        stage_attributes['azure_authentication_method'] = authentication_method
    else:
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, authentication_method)
        stage_attributes['endpoint_type'] = 'URL'

    _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)


@category('basic')
@azure('datalake')
@pytest.mark.parametrize('authentication_method', ['CLIENT'])
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'}])
def test_application_key(sdc_builder, sdc_executor, deltalake, azure, authentication_method, stage_attributes):
    authentication_method = get_auth_method_config(sdc_builder, authentication_method)
    if Version(sdc_builder.version) < Version("5.7.0"):
        stage_attributes['azure_authentication_method'] = authentication_method
    else:
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, authentication_method)
        stage_attributes['endpoint_type'] = 'URL'

    _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)


@category('basic')
@azure('datalake')
@pytest.mark.parametrize('authentication_method', ['CLIENT'])
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'}])
def test_auth_token_endpoint(sdc_builder, sdc_executor, deltalake, azure, authentication_method, stage_attributes):
    authentication_method = get_auth_method_config(sdc_builder, authentication_method)
    if Version(sdc_builder.version) < Version("5.7.0"):
        stage_attributes['azure_authentication_method'] = authentication_method
    else:
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, authentication_method)
        stage_attributes['endpoint_type'] = 'URL'

    _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)


@category('basic')
@aws('s3')
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'AWS_S3'}])
def test_aws_access_key(sdc_builder, sdc_executor, stage_attributes, deltalake, aws):
    _test_with_aws_s3_storage(sdc_builder, sdc_executor, deltalake, aws, stage_attributes)


@category('basic')
@aws('s3')
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'AWS_S3'}])
def test_aws_secret_key(sdc_builder, sdc_executor, stage_attributes, deltalake, aws):
    _test_with_aws_s3_storage(sdc_builder, sdc_executor, deltalake, aws, stage_attributes)


@category('basic')
@azure('datalake')
@pytest.mark.parametrize('authentication_method', ['CLIENT', 'SHARED_KEY'])
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'}])
def test_authentication_method(sdc_builder, sdc_executor, deltalake, azure, authentication_method, stage_attributes):
    authentication_method = get_auth_method_config(sdc_builder, authentication_method)
    if Version(sdc_builder.version) < Version("5.7.0"):
        stage_attributes['azure_authentication_method'] = authentication_method
    else:
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, authentication_method)
        stage_attributes['endpoint_type'] = 'URL'

    _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)


@stub
@category('advanced')
def test_connection_timeout_in_seconds(sdc_builder, sdc_executor, deltalake):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'enable_parallel_queries': False}, {'enable_parallel_queries': True}])
def test_enable_parallel_queries(sdc_builder, sdc_executor, stage_attributes, deltalake):
    pass


@stub
@category('advanced')
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor, deltalake):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'include_query_result_count_in_events': False},
                                              {'include_query_result_count_in_events': True}])
def test_include_query_result_count_in_events(sdc_builder, sdc_executor, stage_attributes, deltalake):
    pass


@stub
@category('advanced')
def test_init_query(sdc_builder, sdc_executor, deltalake):
    pass


@category('basic')
def test_jdbc_connection_string(sdc_builder, sdc_executor, deltalake):
    _test_with_no_storage(sdc_builder, sdc_executor, deltalake,
                          stage_attributes={'storage_location': DEFAULT_STORAGE_LOCATION})


@stub
@category('advanced')
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor, deltalake):
    pass


@stub
@category('advanced')
def test_maximum_pool_size(sdc_builder, sdc_executor, deltalake):
    pass


@stub
@category('advanced')
def test_minimum_idle_connections(sdc_builder, sdc_executor, deltalake):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes, deltalake):
    pass


@stub
@category('basic')
def test_preconditions(sdc_builder, sdc_executor, deltalake):
    pass


@stub
@category('basic')
def test_required_fields(sdc_builder, sdc_executor, deltalake):
    pass


@category('basic')
def test_spark_sql_query(sdc_builder, sdc_executor, deltalake):
    _test_with_no_storage(sdc_builder, sdc_executor, deltalake,
                          stage_attributes={'storage_location': DEFAULT_STORAGE_LOCATION})


@category('basic, smoke')
@aws('s3')
@azure('datalake')
@pytest.mark.parametrize('stage_attributes', [{'storage_location': 'ADLS_GEN2'},
                                              {'storage_location': 'AWS_S3'},
                                              {'storage_location': 'NONE'}])
def test_storage_location(sdc_builder, sdc_executor, stage_attributes, deltalake, aws, azure):
    if stage_attributes['storage_location'] == 'NONE':
        _test_with_no_storage(sdc_builder, sdc_executor, deltalake,
                              stage_attributes={'storage_location': DEFAULT_STORAGE_LOCATION})
    elif stage_attributes['storage_location'] == 'ADLS_GEN2':
        set_sdc_stage_config(deltalake, ADLS_GEN2_AUTH_METHOD_CONFIG, 'SHARED_KEY')
        if Version(sdc_builder.version) >= Version("5.7.0"):
            stage_attributes['endpoint_type'] = 'URL'
        _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes)
    elif stage_attributes['storage_location'] == 'AWS_S3':
        _test_with_aws_s3_storage(sdc_builder, sdc_executor, deltalake, aws, stage_attributes)


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'transaction_isolation': 'DEFAULT'},
                                              {'transaction_isolation': 'TRANSACTION_READ_COMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_READ_UNCOMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_REPEATABLE_READ'},
                                              {'transaction_isolation': 'TRANSACTION_SERIALIZABLE'}])
def test_transaction_isolation(sdc_builder, sdc_executor, stage_attributes, deltalake):
    pass


@category('basic')
def test_without_credentials(sdc_builder, sdc_executor, deltalake):
    stage_attributes = {'storage_location': DEFAULT_STORAGE_LOCATION}
    if Version(sdc_builder.version) < Version('5.7.0'):
        stage_attributes.update({'use_credentials': False})
    _test_with_no_storage(sdc_builder, sdc_executor, deltalake, stage_attributes)


@category('basic')
def test_user_token(sdc_builder, sdc_executor, deltalake):
    _test_with_use_credentials_true(sdc_builder, sdc_executor, deltalake)


def _test_with_use_credentials_true(sdc_builder, sdc_executor, deltalake, stage_attributes={}):
    if Version(sdc_builder.version) < Version('5.7.0'):
        stage_attributes.update({'jdbc_connection_string': deltalake.jdbc_connection_string.split('UID')[0],
                                 'username': deltalake.user,
                                 'user_token': deltalake.password})
    else:
        stage_attributes.update({'jdbc_url': deltalake.jdbc_connection_string.split('UID')[0],
                                 'token': deltalake.password})

    stage_attributes.update({'storage_location': DEFAULT_STORAGE_LOCATION})

    _test_with_no_storage(sdc_builder, sdc_executor, deltalake, stage_attributes,
                          apply_configure_for_environment=False)


def _test_with_no_storage(sdc_builder, sdc_executor, deltalake, stage_attributes=None,
                          apply_configure_for_environment=True):
    """Test for Databricks Delta lake origin where storage=None.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string(string.ascii_lowercase, 10)}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # Databricks Delta lake destination stage
    sql_query = (f"INSERT INTO {table_name} VALUES ('${{record:value('/title')}}', '${{record:value('/author')}}', "
                 "'${record:value('/genre')}', '${record:value('/publisher')}')")
    databricks_deltalake = pipeline_builder.add_stage('Databricks Query', type='executor')

    dev_raw_data_source >> databricks_deltalake

    databricks_deltalake.set_attributes(spark_sql_query=[sql_query],
                                        **stage_attributes if stage_attributes else {})

    pipeline = pipeline_builder.build()
    if apply_configure_for_environment:
        pipeline.configure_for_environment(deltalake)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())
        assert data_from_database == [(record['title'], record['author'], record['genre'], record['publisher'])
                                      for record in ROWS_IN_DATABASE]
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


def _test_with_adls_gen2_storage(sdc_builder, sdc_executor, deltalake, azure, stage_attributes=None):
    """Test for Databricks Delta lake origin that uses ADLS Gen2 as storage.
    Expression evaluator is used for converting map to list-map to write out csv.
    Reason to choose csv format is because it’s the most performant as compared to other formats.

    The pipeline looks like this:
        dev_raw_data_source >> Expression_evaluator >> adls_gen2_destination >= databricks_deltalake
    """
    if not azure:
        pytest.skip('Dtabricks Delta Lake tests with ADLS Gen2 storage require azure object to be set up')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 10)}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')

    # ADLS gen2 storage destination
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = get_random_string(string.ascii_letters, 10)
    adls_gen2_destination = pipeline_builder.add_stage(name=ADLS_GEN2_STAGE_NAME)
    adls_gen2_destination.set_attributes(data_format='DELIMITED',
                                         delimiter_format='POSTGRES_CSV',
                                         header_line='WITH_HEADER',
                                         directory_template=f'/{directory_name}',
                                         files_prefix=files_prefix,
                                         files_suffix=files_suffix)

    # Databricks Delta lake destination stage
    sql_query = (f"COPY INTO {table_name} FROM 'abfss://{azure.datalake.filesystem_id}@"
                 f"{azure.datalake_store_account_fqdn}/${{record:value('/filepath')}}' "
                 "FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true')")
    databricks_deltalake = pipeline_builder.add_stage('Databricks Query', type='executor')
    databricks_deltalake.set_attributes(spark_sql_query=[sql_query],
                                        **stage_attributes if stage_attributes else {})

    dev_raw_data_source >> expression_evaluator >> adls_gen2_destination >= databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, azure)

    try:
        logger.info('Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # assert data from deltalake table is same as what was input
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())
        assert data_from_database == [(record['title'], record['author'], record['genre'], record['publisher'])
                                      for record in ROWS_IN_DATABASE]
        result.close()
    finally:
        try:
            azure.datalake.delete_adls_gen2_entities(directory_name)
        except Exception as ex:
            logger.error(f'Error encountered while deleting ADLS Gen2 entities in directory = {directory_name} as {ex}')
        _clean_up_databricks(deltalake, table_name)


def _test_with_aws_s3_storage(sdc_builder, sdc_executor, deltalake, aws, stage_attributes=None):
    """Test for Databricks Delta lake origin with AWS S3 storage.
    Expression evaluator is used for converting map to list-map to write out csv.
    Reason to choose csv format is because it’s the most performant as compared to other formats.

    The pipeline looks like this:
        dev_raw_data_source >> Expression_evaluator >> s3_destination >= databricks_deltalake
    """
    if not aws:
        pytest.skip('Dtabricks Delta Lake tests with AWS S3 storage require aws object to be set up')
    table_name = f'stf_{get_random_string(string.ascii_lowercase, 10)}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')

    # AWS S3 destination
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_COMMON_PREFIX}/{get_random_string(string.ascii_letters, 10)}'
    s3_destination = pipeline_builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket=s3_bucket, common_prefix=s3_key,
                                  data_format='DELIMITED', delimiter_format='POSTGRES_CSV', header_line='WITH_HEADER')

    # Databricks Delta lake destination stage
    sql_query = (f"COPY INTO {table_name} FROM 's3a://${{record:value('/bucket')}}/${{record:value('/objectKey')}}' "
                 "FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true')")
    databricks_deltalake = pipeline_builder.add_stage('Databricks Query', type='executor')
    databricks_deltalake.set_attributes(spark_sql_query=[sql_query],
                                        include_query_result_count_in_events=True,
                                        **stage_attributes if stage_attributes else {})

    dev_raw_data_source >> expression_evaluator >> s3_destination >= databricks_deltalake

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
        assert data_from_database == [(record['title'], record['author'], record['genre'], record['publisher'])
                                      for record in ROWS_IN_DATABASE]
        result.close()
    finally:
        try:
            aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        except Exception as ex:
            logger.error(
                f'Error encountered while deleting AWS S3 entities in bucket = {s3_bucket} and key = {s3_key} as {ex}')
        _clean_up_databricks(deltalake, table_name)
