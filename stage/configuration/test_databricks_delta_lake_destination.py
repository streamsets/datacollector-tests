#  Copyright (c) 2023 StreamSets Inc.

import json
import logging
import pytest
from .. import _clean_up_databricks
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import aws, deltalake, sdc_min_version
from streamsets.testframework.utils import get_random_string

DESTINATION_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_DatabricksDeltaLakeDTarget'

pytestmark = [deltalake, sdc_min_version('5.5.0')]

logger = logging.getLogger(__name__)

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
def test_directory_for_table_location(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.
    Verifies that the propeerty for table location respects where the table is created

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
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        directory_for_table_location=table_location)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=180)

        # Assert data from deltalake table is same as what was input.
        connection = engine.connect()
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
        connection = engine.connect()
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in output_data]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [tuple(record.values()) for record in data_from_database]
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@stub
def test_null_value(sdc_builder, sdc_executor):
    pass


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
                          ("TO_ERROR"     , _start_pipeline_and_check_to_error),
                          ("DISCARD"      , _start_pipeline_and_check_discard)])
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
        connection = engine.connect()
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
        connection = engine.connect()
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

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    # In case of IAM Roles we set it to True and set keys to blank
    if use_instance_profile:
        databricks_deltalake.set_attributes(use_instance_profile=True, access_key_id="", secret_access_key="")

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = engine.connect()
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
        connection = engine.connect()
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
        connection = engine.connect()
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
        connection = engine.connect()
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
        connection = engine.connect()
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
