#  Copyright (c) 2023 StreamSets Inc.
import copy
import json
import logging
import os
import random
import string
import tempfile
import time
import pytest
import re
from operator import itemgetter
from stage import _clean_up_databricks
from streamsets.sdk.exceptions import StartError, StartingError, RunError, RunningError, ValidationError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import aws, azure, deltalake, gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

DESTINATION_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_DatabricksDeltaLakeDTarget'
MIN_VERSION_UNITY_CATALOG = 11
MIN_VERSION_CLUSTER = 6
REGEX_EXPRESSION_DATABRICKS_VERSION = re.compile(r'\d+')

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

ROWS_IN_DATABASE_PARQUET = [
    {"title": "Elon Musk: Tesla SpaceX and the Quest for a Fantastic Future",
     "author": "Ashlee Vance",
     "genre": "Biography",
     "publisher": "HarperCollins Publishers"},
    {"title": "Steve Jobs",
     "author": "Walter Isaacson",
     "genre": "Biography",
     "publisher": "Simon & Schuster"},
    {"title": "The Spy and the Traitor: The Greatest Espionage Story of the Cold War",
     "author": "Ben Macintyre",
     "genre": "Biography True crime",
     "publisher": "McClelland & Stewart"}
]

ROWS_IN_DATABASE_QUOTING = [
    {"title": "\"Elon Musk: Tesla, SpaceX, the Quest for a Fantastic Future\"",
     "author": "Alex",
     "genre": "Escalation",
     "publisher": "StreamSets"}
]

ROWS_IN_DATABASE_QUOTING_AND_ESCAPING = [
    {"title": "\"Elon Musk: Tesla, SpaceX, the Quest for a Fantastic Future\"",
     "author": "Alex = Developer",
     "genre": "Escalation costs $ and €",
     "publisher": "StreamSets"}
]

CDC_ROWS_IN_DATABASE = [
    {'OP': 1, 'NAME': 'Alex Sanchez', 'ROLE': 'Developer', 'AGE': 27, 'TEAM': 'Cloud'},
    {'OP': 1, 'NAME': 'Xavi Baques', 'ROLE': 'Developer', 'AGE': 27, 'TEAM': 'Platform'},
    {'OP': 1, 'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'Connectivity BCN'},
    {'OP': 1, 'NAME': 'Random person', 'ROLE': 'Prospect', 'AGE': 99, 'TEAM': 'Undefined'},
    {'OP': 1, 'NAME': 'Tucu', 'ROLE': 'Distinguished Developer', 'AGE': 50, 'TEAM': 'Innovation'},
    {'OP': 2, 'NAME': 'Random person'},  # Remove prospect
    {'OP': 4, 'NAME': 'Alex Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},  # Upsert Role and Team
    {'OP': 3, 'NAME': 'Xavi Baques', 'ROLE': 'Tech Lead', 'AGE': 28, 'TEAM': 'Enterprise'},  # Update Role and Team
    {'OP': 3, 'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'BCN'}  # Update Team
]

CDC_RESULT_ROWS = [
    {'NAME': 'Alex Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},
    {'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'BCN'},
    {'NAME': 'Tucu', 'ROLE': 'Distinguished Developer', 'AGE': 50, 'TEAM': 'Innovation'},
    {'NAME': 'Xavi Baques', 'ROLE': 'Tech Lead', 'AGE': 28, 'TEAM': 'Enterprise'},
]

ROWS_FOR_DRIFT = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 3, 'name': 'Dominic Thiem'}
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

AVRO_SCHEMA = '{ "type" : "record", "name" : "test_schema", "doc" : "", \
                 "fields" : [ \
                     { "name" : "title", "type" : "string" }, \
                     { "name" : "author", "type" : "string" }, \
                     { "name" : "genre", "type" : "string" }, \
                     { "name" : "publisher", "type" : "string" } ] }'


def set_sdc_stage_config(deltalake, config, value):
    # There is this stf issue that sets up 2 configs are named the same, both configs are set up
    # If the config is an enum, it created invalid pipelines (e.g. Authentication Method in azure and s3 staging)
    # This acts as a workaround to only set that specific config
    custom_deltalake = copy.deepcopy(deltalake)
    custom_deltalake.sdc_stage_configurations[DESTINATION_STAGE_NAME][config] = value
    return custom_deltalake


@aws('s3')
@pytest.mark.parametrize('specify_region', [False, True])
@pytest.mark.parametrize('use_instance_profile', [False, True])
def test_with_aws_s3_storage(sdc_builder, sdc_executor, deltalake, aws, use_instance_profile, specify_region):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
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
                                        purge_stage_file_after_ingesting=True)

    if specify_region:
        if Version(sdc_builder.version) < Version("5.7.0"):
            databricks_deltalake.set_attributes(specify_aws_region=specify_region,
                                                aws_region=aws.region.upper().replace('-', '_'))
        elif Version(sdc_executor.version) < Version("6.1.0"):
            databricks_deltalake.set_attributes(use_specific_region=specify_region,
                                                region=aws.region.upper().replace('-', '_'))
        else:
            databricks_deltalake.set_attributes(
                region_definition_for_s3="SPECIFY_REGION",
                region_for_s3=aws.region.upper().replace('-', '_')
            )

    # In case of Instance Profile we set it to True and set keys to blank
    if use_instance_profile:
        if Version(sdc_builder.version) < Version("5.7.0"):
            databricks_deltalake.set_attributes(use_instance_profile=True, access_key_id="", secret_access_key="")
        else:
            aws_config_prefix = 'config.s3Stage.connection.awsConfig'
            deltalake = set_sdc_stage_config(deltalake, f"{aws_config_prefix}.credentialMode", 'WITH_IAM_ROLES')
            deltalake = set_sdc_stage_config(deltalake, f"{aws_config_prefix}.awsAccessKeyId", '')
            deltalake = set_sdc_stage_config(deltalake, f"{aws_config_prefix}.awsSecretAccessKey", '')

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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('specify_region', [
    None,
    'use_region',
    'use_custom_region',
    'use_regional_endpoint',
    'use_regional_vpc_endpoint',
    'use_custom_endpoint_and_signing_region',
    'use_custom_endpoint_and_custom_signing_region'
])
def test_with_aws_s3_storage_using_different_region_configurations(
        sdc_builder,
        sdc_executor,
        deltalake,
        aws,
        specify_region
):
    """Test for Databricks Delta Lake with AWS S3 storage using different S3 region configurations.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
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
                                        purge_stage_file_after_ingesting=True
                                        )

    if specify_region == 'use_region':
        databricks_deltalake.set_attributes(
            region_definition_for_s3="SPECIFY_REGION",
            region_for_s3=aws.formatted_region
        )
    elif specify_region == 'use_custom_region':
        databricks_deltalake.set_attributes(
            region_definition_for_s3="SPECIFY_REGION",
            region_for_s3="OTHER",
            custom_region_for_s3=aws.region
        )
    elif specify_region == 'use_regional_endpoint':
        databricks_deltalake.set_attributes(
            region_definition_for_s3="SPECIFY_REGIONAL_ENDPOINT",
            regional_endpoint_for_s3=f"s3.{aws.region}.amazonaws.com"
        )
    elif specify_region == 'use_regional_vpc_endpoint':
        databricks_deltalake.set_attributes(
            region_definition_for_s3="SPECIFY_REGIONAL_ENDPOINT",
            regional_endpoint_for_s3=aws.vpc_endpoint
        )
    elif specify_region == 'use_custom_endpoint_and_signing_region':
        databricks_deltalake.set_attributes(
            region_definition_for_s3="SPECIFY_NON_REGIONAL_ENDPOINT",
            custom_endpoint_for_s3=aws.vpc_endpoint,
            signing_region_for_s3=aws.formatted_region
        )
    elif specify_region == 'use_custom_endpoint_and_custom_signing_region':
        databricks_deltalake.set_attributes(
            region_definition_for_s3="SPECIFY_NON_REGIONAL_ENDPOINT",
            custom_endpoint_for_s3=aws.vpc_endpoint,
            signing_region_for_s3="OTHER",
            custom_signing_region_for_s3=aws.region
        )
    else:
        databricks_deltalake.set_attributes(
            region_definition_for_s3="NOT_SPECIFIED",
        )

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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('specify_sts_region', [
    None,
    'use_region',
    'use_custom_region',
    'use_regional_endpoint',
    'use_regional_vpc_endpoint',
    'use_custom_endpoint_and_signing_region',
    'use_custom_endpoint_and_custom_signing_region'
])
@pytest.mark.parametrize('use_instance_profile', [False, True])
def test_with_aws_s3_storage_using_assume_role(
        sdc_builder,
        sdc_executor,
        deltalake,
        aws,
        use_instance_profile,
        specify_sts_region
):
    """Test for Databricks Delta Lake with AWS S3 storage using Assume Role with different STS region configuration.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    data = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
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
                                        purge_stage_file_after_ingesting=True,
                                        assume_role=True,
                                        role_arn=aws.iam_role
                                        )

    if specify_sts_region == 'use_region':
        databricks_deltalake.set_attributes(
            region_definition_for_sts="SPECIFY_REGION",
            region_for_sts=aws.formatted_region
        )
    elif specify_sts_region == 'use_custom_region':
        databricks_deltalake.set_attributes(
            region_definition_for_sts="SPECIFY_REGION",
            region_for_sts="OTHER",
            custom_region_for_sts=aws.region
        )
    elif specify_sts_region == 'use_regional_endpoint':
        databricks_deltalake.set_attributes(
            region_definition_for_sts="SPECIFY_REGIONAL_ENDPOINT",
            regional_endpoint_for_sts=f"sts.{aws.region}.amazonaws.com"
        )
    elif specify_sts_region == 'use_regional_vpc_endpoint':
        databricks_deltalake.set_attributes(
            region_definition_for_sts="SPECIFY_REGIONAL_ENDPOINT",
            regional_endpoint_for_sts=aws.sts_vpc_endpoint
        )
    elif specify_sts_region == 'use_custom_endpoint_and_signing_region':
        databricks_deltalake.set_attributes(
            region_definition_for_sts="SPECIFY_NON_REGIONAL_ENDPOINT",
            custom_endpoint_for_sts=aws.sts_vpc_endpoint,
            signing_region_for_sts=aws.formatted_region
        )
    elif specify_sts_region == 'use_custom_endpoint_and_custom_signing_region':
        databricks_deltalake.set_attributes(
            region_definition_for_sts="SPECIFY_NON_REGIONAL_ENDPOINT",
            custom_endpoint_for_sts=aws.sts_vpc_endpoint,
            signing_region_for_sts="OTHER",
            custom_signing_region_for_sts=aws.region
        )
    else:
        databricks_deltalake.set_attributes(
            region_definition_for_sts="NOT_SPECIFIED",
        )

    # In case of Instance Profile we set it to True and set keys to blank
    if use_instance_profile:
        aws_config_prefix = 'config.s3Stage.connection.awsConfig'
        deltalake = set_sdc_stage_config(deltalake, f"{aws_config_prefix}.credentialMode", 'WITH_IAM_ROLES')
        deltalake = set_sdc_stage_config(deltalake, f"{aws_config_prefix}.awsAccessKeyId", '')
        deltalake = set_sdc_stage_config(deltalake, f"{aws_config_prefix}.awsSecretAccessKey", '')

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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@azure('datalake')
def test_with_adls_shared_key_storage(sdc_builder, sdc_executor, deltalake, azure):
    """Test for Databricks Delta Lake that uses ADLS Gen2 as storage.

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

    # ADLS gen2 storage destination
    files_prefix = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='ADLS_GEN2',
                                        stage_file_prefix=files_prefix,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True)

    if Version(sdc_builder.version) < Version("5.7.0"):
        databricks_deltalake.set_attributes(azure_authentication_method='SHARED_KEY')
    else:
        deltalake = set_sdc_stage_config(deltalake, 'config.dataLakeGen2Stage.connection.authMethod', 'SHARED_KEY')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, azure)

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


@azure('datalake')
@pytest.mark.parametrize('staging_file_format', ["CSV", "PARQUET"])
def test_with_adls_oauth_storage(sdc_builder, sdc_executor, deltalake, azure, staging_file_format):
    """Test for Databricks Delta Lake that uses ADLS Gen2 as storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """

    if Version(sdc_builder.version) < Version('5.7.0') and staging_file_format == 'PARQUET':
        pytest.skip('PARQUET staging introduced in 5.7.0')

    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    rows_in_db = ROWS_IN_DATABASE_PARQUET if staging_file_format == "PARQUET" else ROWS_IN_DATABASE
    DATA = '\n'.join(json.dumps(rec) for rec in rows_in_db)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # ADLS gen2 storage destination
    files_prefix = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='ADLS_GEN2',
                                        on_record_error='STOP_PIPELINE',
                                        stage_file_prefix=files_prefix,
                                        table_name=table_name,
                                        staging_file_format=staging_file_format,
                                        purge_stage_file_after_ingesting=True)

    if Version(sdc_builder.version) < Version("5.7.0"):
        databricks_deltalake.set_attributes(azure_authentication_method='OAUTH')
    else:
        deltalake = set_sdc_stage_config(deltalake, 'config.dataLakeGen2Stage.connection.authMethod', 'CLIENT')
        databricks_deltalake.set_attributes(parquet_schema_location='INFER',
                                            endpoint_type='URL')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, azure)

    try:
        logger.info(f'Creating table {table_name} ...')
        deltalake.create_table(table_name)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in rows_in_db]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('auto_create_table', [True, False])
@pytest.mark.parametrize('include_database', [True, False])
def test_with_aws_s3_storage_cdc(sdc_builder, sdc_executor, deltalake, aws, auto_create_table, include_database):
    """Test for Databricks Delta lake with AWS S3 storage. Using CDC data as input

    The pipeline looks like this:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> databricks_deltalake
    """
    if include_database:
        # TODO DATABRICKS-89 Change test to create and drop database instead of hardcoding it
        table_name = f'stf_database.stf_{get_random_string()}'
    else:
        table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in CDC_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location="AWS_S3",
                                        stage_file_prefix=s3_key)
    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=auto_create_table,
                                        auto_create_table=auto_create_table,
                                        merge_cdc_data=True,
                                        primary_key_location="TABLE",
                                        table_key_columns=[{
                                            "keyColumns": [
                                                "NAME"
                                            ],
                                            "table": table_name
                                        }])

    dev_raw_data_source >> expression_evaluator >> field_remover >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)
        if not auto_create_table:
            logger.info(f'Creating table {table_name} ...')
            query = f'create table {table_name} (NAME string, ROLE string, AGE integer, TEAM string) using DELTA'
            connection.execute(query)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=180)

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in CDC_RESULT_ROWS]

        assert len(data_from_database) == len(expected_data)

        if auto_create_table:
            table_def = connection.execute(f'Describe {table_name}')
            assert [column in table_def.fetchall() for column in
                    [('name', 'string', None), ('role', 'string', None), ('age', 'int', None),
                     ('team', 'string', None)]]
            table_def.close()

            assert expected_data == [(record['name'], record['role'], record['age'], record['team'])
                                     for record in data_from_database]
        else:
            assert expected_data == [(record['NAME'], record['ROLE'], record['AGE'], record['TEAM'])
                                     for record in data_from_database]

        result.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
def test_cdc_deltalake_multiple_ops_two_batches(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Deltalake destination target stage. Data is inserted into Deltalake using the pipeline.
    After pipeline is run, data is read from Deltalake using sqlalchemy client.
    We insert data in two runs.
    We assert the data from the client to what has been ingested by the Deltalake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field and a table name
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Deltalake stage adds the data

    The pipeline looks like:
    Deltalake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> deltalake_destination

    This test is for DATABRICKS-125, it set up two batches
    -- Bacth 0
        UPINSERT A
        UPINSERT A

    -- Batch 1
        UPINSERT A
        UPINSERT A
        DELETE A

    """

    table_name = f'stf_{get_random_string()}'
    CDC_ROWS_MULT_OPS_1 = [
        {'OP': 4, 'ID': 1, 'NAME': 'Rogelio Federer'},
        {'OP': 4, 'ID': 1, 'NAME': 'Rafa Nadal'}]
    CDC_ROWS_MULT_OPS_2 = [
        {'OP': 4, 'ID': 1, 'NAME': 'Domi Thiem'},
        {'OP': 4, 'ID': 1, 'NAME': 'Juan Del Potro'},
        {'OP': 2, 'ID': 1, 'NAME': 'Juan Del Potro'}]

    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()
    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in CDC_ROWS_MULT_OPS_1))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])
    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']
    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'
    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location="AWS_S3",
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        merge_cdc_data=True,
                                        primary_key_location="TABLE",
                                        table_key_columns=[{
                                            "keyColumns": [
                                                "ID"
                                            ],
                                            "table": table_name
                                        }])

    dev_raw_data_source >> expression_evaluator >> field_remover >> databricks_deltalake
    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    try:
        connection = deltalake.connect_engine(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=180)

        # We assert data in Delta Lake is just one row the second
        # Upsert + Upsert = Second Upsert
        result = connection.execute(f'select ID, NAME from {table_name}')
        data_from_database = sorted(result.fetchall())
        assert data_from_database == [(row['ID'], row['NAME']) for row in CDC_ROWS_MULT_OPS_1[1:2]]

        table_def = connection.execute(f'Describe {table_name}')

        assert [column in table_def.fetchall() for column in [('id', 'int', None), ('name', 'string', None)]]

        table_def.close()
        result.close()

        # We assert data in Delta Lake is empty
        # Upsert + Upsert + Delete = []
        raw_data = '\n'.join((json.dumps(row) for row in CDC_ROWS_MULT_OPS_2))
        pipeline[0].configuration.update({'rawData': raw_data})
        sdc_executor.update_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == []
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('staging_format', ['CSV', 'AVRO - IN LINE', 'AVRO - HEADER', 'AVRO - INFER'])
def test_insert_multiple_types(sdc_builder, sdc_executor, deltalake, aws, staging_format):
    """Test for Databricks Delta lake with AWS S3 storage.
    We test that basic data types can be handled by the stage
    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake

    In case of 'AVRO - HEADER' it will include also a schema_generator
    """
    table_name = f'stf_{get_random_string()}'
    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'integerField', 'type': 'INTEGER'},
        {'field': 'longField', 'type': 'LONG'},
        {'field': 'floatField', 'type': 'FLOAT'},
        {'field': 'doubleField', 'type': 'DOUBLE'},
        {'field': 'decimalField', 'precision': 10, 'scale': 2, 'type': 'DECIMAL'},
        {'field': 'stringField', 'type': 'STRING'},
        {'field': 'binaryField', 'type': 'BYTE_ARRAY'},
        {'field': 'booleanField', 'type': 'BOOLEAN'},
        {'field': 'dateField', 'type': 'DATE'},
        {'field': 'datetimeField', 'type': 'DATETIME'},
    ]
    batch_size = 50
    dev_data_generator.set_attributes(delay_between_batches=1000, batch_size=batch_size)
    # AWS S3 destination
    s3_key = f'deltalake-{get_random_string()}'
    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=False,
                                        auto_create_table=False)

    wiretap = pipeline_builder.add_wiretap()

    if staging_format == 'AVRO':
        avro_schema = {
            "type": "record",
            "name": "DeltaLake",
            "fields": [
                {"name": "integerField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "decimalField",
                 "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "stringField", "type": "string"},
                {"name": "binaryField", "type": "bytes"},
                {"name": "booleanField", "type": "boolean"},
                {"name": "dateField", "type": {"type": "int", "logicalType": "date"}},
                {"name": "datetimeField", "type": {"type": "long", "logicalType": "timestamp-micros"}}
            ]
        }

        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='INLINE',
                                            avro_schema=json.dumps(avro_schema))
    elif staging_format == 'AVRO - HEADER':
        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='HEADER')
    elif staging_format == 'AVRO - INFER':
        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='INFER')

    if staging_format == 'AVRO - HEADER':
        # Basically if we use in-header schema we need to add a schema generator in the middle
        schema_generator = pipeline_builder.add_stage('Schema Generator')
        schema_generator.schema_name = 'DeltaLake'

        dev_data_generator >> schema_generator >> [databricks_deltalake, wiretap.destination]
    else:
        dev_data_generator >> [databricks_deltalake, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        logger.info(f'Creating table {table_name} ...')
        connection = deltalake.connect_engine(engine)
        query = (f'create table {table_name} (integerField integer, longField bigint, floatField float, '
                 'doubleField double, decimalField decimal(10,2), stringField string, binaryField binary, '
                 'booleanField boolean, dateField date, datetimeField timestamp) using DELTA')
        connection.execute(query)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batch_size * 10, timeout_sec=3600)
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()

        time.sleep(30)

        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert len(data_from_database) == len(wiretap.output_records)
        assert len(wiretap.error_records) == 0
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
# Since we infer the schema from dev raw data source there is no option to use header schema
@pytest.mark.parametrize('staging_format', ['CSV', 'AVRO - IN LINE'])
def test_insert_all_types(sdc_builder, sdc_executor, deltalake, aws, staging_format):
    """Test for Databricks Delta Lake with AWS S3 storage.
    We test all types, including complex types and we verify that the value is correct.
    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'
    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Binary, Date, DateTime cannot be replicated with a JSON
    all_types = [
        {'byteField': 125,
         'shortField': 32765,
         'integerField': 2147483645,
         'longField': 9223372036854775805,
         'floatField': 0.42232013,
         'doubleField': 0.2710717403213595,
         'decimalField': 51926281.06,
         'stringField': get_random_string(),
         'booleanField': False}
    ]

    DATA = '\n'.join(json.dumps(rec) for rec in all_types)

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'deltalake-{get_random_string()}'
    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=False,
                                        auto_create_table=False)

    if staging_format == 'AVRO':
        avro_schema = {
            "type": "record",
            "name": "DeltaLake",
            "fields": [
                {"name": "byteField", "type": "int"},
                {"name": "shortField", "type": "int"},
                {"name": "integerField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "booleanField", "type": "boolean"},
            ]
        }

        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='INLINE',
                                            avro_schema=json.dumps(avro_schema))

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    try:
        logger.info(f'Creating table {table_name} ...')
        connection = deltalake.connect_engine(engine)
        query = (f'create table {table_name} (byteField byte, shortField short, integerField integer, '
                 'longField bigint, floatField float, doubleField double, decimalField decimal(10,2), '
                 'stringField string, booleanField boolean) using DELTA')
        connection.execute(query)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in all_types]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(
            record['byteField'], record['shortField'], record['integerField'], record['longField'],
            record['floatField'], record['doubleField'], float(record['decimalField']),
            record['stringField'], record['booleanField'])
            for record in data_from_database]
        result.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('auto_create_table', [True, False])
@pytest.mark.parametrize('create_new_columns_string', [True, False])
# Skipping avro header since the pipeline is built in an external method.
@pytest.mark.parametrize('staging_format', ['CSV', 'AVRO - IN LINE', 'AVRO - INFER'])
def test_data_drift(sdc_builder, sdc_executor, deltalake, aws, auto_create_table, create_new_columns_string,
                    staging_format):
    """Test for Databricks Delta lake target stage. Data is inserted into Delta Lake using the pipeline.
    Two pipelines are run, the second one with a newer column.
    The expected behavior is to have six rows.

    Each pipeline look like:
        dev_raw_data_source  >> databricks_deltalake
    """

    table_name = f'stf_{get_random_string()}'
    engine = deltalake.engine

    def _create_pipeline(rows_to_insert, create_new_columns_as_string):
        """ Auxiliar function that creates a pipeline with a dev_raw_data_source with rows_to_insert
            connected to a databricks_deltalake connector.
            The sdc_builder is used to build the pipeline, the deltalake and aws are used to configure environment.
            enable_drift parameter allows to configure enable_data_drift attribute for deltalake connector.
            auto_create parameter allows to configure auto_create_table in TRUE, when enable_drift is enabled.
            The function also returns the S3_key generated.
        """

        DATA = '\n'.join(json.dumps(rec) for rec in rows_to_insert)
        pipeline_builder = sdc_builder.get_pipeline_builder()

        # Dev raw data source
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

        # AWS S3 destination
        s3_key = f'stf-deltalake/{get_random_string()}'

        # Databricks Delta lake destination stage
        databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
        databricks_deltalake.set_attributes(staging_location="AWS_S3",
                                            stage_file_prefix=s3_key)

        dev_raw_data_source >> databricks_deltalake

        databricks_deltalake.set_attributes(table_name=table_name,
                                            purge_stage_file_after_ingesting=True,
                                            enable_data_drift=True,
                                            auto_create_table=auto_create_table,
                                            create_new_columns_as_string=create_new_columns_as_string)
        created_pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
        return created_pipeline, s3_key

    pipeline, s3_key_1 = _create_pipeline(ROWS_FOR_DRIFT, create_new_columns_as_string=False)

    pipeline_2, s3_key_2 = _create_pipeline(ROWS_FOR_DRIFT_EXT, create_new_columns_as_string=create_new_columns_string)

    if staging_format == 'AVRO - INFER':
        pipeline.stages.get_all()[1].set_attributes(staging_file_format='AVRO',
                                                    avro_schema_location='INFER')
        pipeline_2.stages.get_all()[1].set_attributes(staging_file_format='AVRO',
                                                      avro_schema_location='INFER')

    try:
        logger.info(f'Creating table {table_name} ...')
        connection = deltalake.connect_engine(engine)
        if not auto_create_table:
            query = f'create table {table_name} (id integer, name string) using DELTA'
            connection.execute(query)

        sdc_executor.add_pipeline(pipeline)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)
        except ValidationError as e:
            if create_new_columns_string and staging_format == 'AVRO - INFER':
                assert 'DELTA_LAKE_45' in e.value.message
            elif staging_format == 'AVRO - IN LINE':
                assert 'DELTA_LAKE_44' in e.value.message
            else:
                # Assert data from deltalake table is same as what was input.
                result = connection.execute(f'select * from {table_name}')
                data_from_database = sorted(result.fetchall())

                expected_data = [tuple(v for v in d.values()) for d in ROWS_FOR_DRIFT]
                assert len(data_from_database) == len(expected_data)
                assert expected_data == [(record['id'], record['name'])
                                         for record in data_from_database]
                result.close()

                sdc_executor.add_pipeline(pipeline_2)
                sdc_executor.start_pipeline(pipeline_2).wait_for_finished(timeout_sec=120)

                # Assert that the table was properly created
                if auto_create_table:
                    table_def = connection.execute(f'Describe {table_name}')
                    if create_new_columns_string:
                        assert [column in table_def.fetchall() for column in
                                [('id', 'int', None), ('name', 'string', None), ('ranking', 'string', None)]]
                    else:
                        assert [column in table_def.fetchall() for column in
                                [('id', 'int', None), ('name', 'string', None), ('ranking', 'int', None)]]
                    table_def.close()

                # Assert data from deltalake table is same as what was input.
                result = connection.execute(f'select * from {table_name} where id >= 4')
                data_from_database = sorted(result.fetchall())

                if create_new_columns_string:
                    expected_values = ROWS_FOR_DRIFT_EXT_STRING
                else:
                    expected_values = ROWS_FOR_DRIFT_EXT

                expected_data = [tuple(v for v in d.values()) for d in expected_values]
                assert len(data_from_database) == len(expected_data)
                assert expected_data == [(record['id'], record['name'], record['ranking'])
                                         for record in data_from_database]
                result.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key_1)
        aws.delete_s3_data(aws.s3_bucket_name, s3_key_2)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('table_prefix', ['', f'stf_database.'])
def test_el_eval_tablename(sdc_builder, sdc_executor, deltalake, aws, table_prefix):
    """Test for Databricks Delta Lake with AWS S3 storage.
    Verify that ELs can be used in the table name property

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name_1 = f'{table_prefix}STF_TABLE_{get_random_string()}'
    table_name_2 = f'{table_prefix}STF_TABLE_{get_random_string()}'

    data = [
        {'TABLE': table_name_1, 'ID': 1, 'NAME': 'Rogelio Federer'},
        {'TABLE': table_name_2, 'ID': 2, 'NAME': 'Rafa Nadal'},
        {'TABLE': table_name_1, 'ID': 3, 'NAME': 'Domi Thiem'},
        {'TABLE': table_name_2, 'ID': 4, 'NAME': 'Juan Del Potro'},
        {'TABLE': table_name_1, 'ID': 1, 'NAME': 'Roger Federer'},
        {'TABLE': table_name_2, 'ID': 2, 'NAME': 'Rafael Nadal'},
        {'TABLE': table_name_1, 'ID': 3, 'NAME': 'Dominic Thiem'},
        {'TABLE': table_name_2, 'ID': 4, 'NAME': 'Juan Del Potro'}
    ]

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in data)
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
    databricks_deltalake.set_attributes(table_name="${record:value('/TABLE')}",
                                        purge_stage_file_after_ingesting=True,
                                        replace_newlines=True)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)
        logger.info(f'Creating table {table_name_1} ...')
        query = f'create table {table_name_1} (table string, id integer, name string) using DELTA'
        connection.execute(query)
        logger.info(f'Creating table {table_name_2} ...')
        query = f'create table {table_name_2} (table string, id integer, name string) using DELTA'
        connection.execute(query)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        connection = deltalake.connect_engine(engine)
        result_1 = connection.execute(f'select * from {table_name_1}')
        result_2 = connection.execute(f'select * from {table_name_2}')
        data_from_database_1 = sorted(result_1.fetchall())
        data_from_database_2 = sorted(result_2.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in data]

        assert len(data_from_database_1) + len(data_from_database_2) == len(expected_data)

        expected_data_1 = [tuple(d.values()) for d in data if d['TABLE'] == table_name_1]
        expected_data_2 = [tuple(d.values()) for d in data if d['TABLE'] == table_name_2]

        assert [(record['table'], record['id'], record['name']) for record in
                sorted(data_from_database_1)] == sorted(expected_data_1)
        assert [(record['table'], record['id'], record['name']) for record in
                sorted(data_from_database_2)] == sorted(expected_data_2)

        result_1.close()
        result_2.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name_1)
        _clean_up_databricks(deltalake, table_name_2)


@aws('s3')
def test_table_el_eval_multiple_threads(sdc_builder, sdc_executor, deltalake, aws):
    """Test Delta Lake destination is able to split into different tables based on EL using different threads
    - A directory origin stage generates the data
    - A Delta lake stage tries to send the data but records go to error

    The pipeline looks like:
    DeltaLake pipeline:
        directory >> databricks_deltalake
        directory >= file_finished_finisher
    """
    table_name = f'stf_table_{get_random_string(string.ascii_lowercase, 5)}'
    table_names = []
    create_queries = []
    select_queries = []

    for i in range(4):
        table_names.append(f'{table_name}_{i}')
        create_queries.append(f'create table {table_names[i]} (id integer, name string) using DELTA')
        select_queries.append(f'select * from {table_names[i]}')

    data = []

    engine = deltalake.engine

    for i in range(0, 250):
        data.append({'table': f'{table_name}_{random.randint(1, 5)}', 'id': i, 'name': get_random_string()})

    raw_data = '\n'.join((json.dumps(row) for row in data))

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())

    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')
    sdc_executor.write_file(os.path.join(tmp_directory, 'sdc1.txt'), raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='JSON', file_name_pattern='sdc*.txt', file_name_pattern_mode='GLOB',
                             files_directory=tmp_directory, process_subdirectories=True, read_order='TIMESTAMP',
                             batch_size_in_recs=50, number_of_threads=5)

    file_finished_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    file_finished_finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name="${record:value('/table')}",
                                        purge_stage_file_after_ingesting=True,
                                        replace_newlines=True)

    if Version(sdc_builder.version) < Version("5.7.0"):
        databricks_deltalake.set_attributes(connection_pool_size=5)
    else:
        databricks_deltalake.set_attributes(maximum_connection_threads=5)

    directory >> databricks_deltalake
    directory >= file_finished_finisher

    pipeline = (pipeline_builder.build(title='Directory to Databricks using Els')).configure_for_environment(deltalake)
    sdc_executor.add_pipeline(pipeline)

    try:
        connection = deltalake.connect_engine(engine)
        for query in create_queries:
            connection.execute(query)

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=600)

        for i in range(4):
            connection = deltalake.connect_engine(engine)
            result = connection.execute(f'{select_queries[i]}')
            data_from_database = sorted(result.fetchall())

            inserted_data = [(row['id'], row['name'], row['table']) for row in
                             list(filter(lambda x: x['table'] == f'{table_names[i]}', data))]
            assert len(inserted_data) == len(data_from_database)
            assert inserted_data == data_from_database
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        for i in range(1, 5):
            _clean_up_databricks(deltalake, f'{table_name}_{i}')


@aws('s3')
def test_with_special_quoting(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_QUOTING)
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
                                        quote_character="|",
                                        column_separator=":")

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

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE_QUOTING]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
def test_with_quoting_mode_both(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE_QUOTING_AND_ESCAPING)
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
                                        quoting_mode='BOTH',
                                        quote_character="\"",
                                        escape_character="\\",
                                        column_separator=",")

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

        expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE_QUOTING_AND_ESCAPING]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                 for record in data_from_database]
        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
def test_pre_created_partitioned_table(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with a pre-created partitioned table

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    create_table_query = f"CREATE TABLE {table_name} (title STRING, author STRING, genre STRING, publisher STRING) " \
                         f"USING DELTA PARTITIONED BY (genre)"

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

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)
        logger.info(f'Creating table {table_name} ...')
        connection.execute(create_table_query)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('pre_created_table', [True, False])
def test_cdc_with_partitioned_table(sdc_builder, sdc_executor, deltalake, aws, pre_created_table):
    """Test for Databricks Delta lake with AWS S3 storage. Using CDC data as input, writting to a partitioned table

    The pipeline looks like this:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in CDC_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location="AWS_S3",
                                        stage_file_prefix=s3_key,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=False,
                                        auto_create_table=False,
                                        merge_cdc_data=True,
                                        primary_key_location="TABLE",
                                        table_key_columns=[{
                                            "keyColumns": [
                                                "NAME"
                                            ],
                                            "table": table_name
                                        }])

    if not pre_created_table:
        databricks_deltalake.set_attributes(enable_data_drift=True,
                                            auto_create_table=True,
                                            partition_table=True,
                                            partition_columns=[{'tableName': table_name, 'columnName': 'team'}])

    dev_raw_data_source >> expression_evaluator >> field_remover >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)
        if pre_created_table:
            logger.info(f'Creating table {table_name} ...')
            connection.execute(f'create table {table_name} (name string, role string, age integer, team string) using '
                               f'DELTA PARTITIONED BY (team)')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=180)

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        expected_data = [tuple(v for v in d.values()) for d in CDC_RESULT_ROWS]

        assert len(data_from_database) == len(expected_data)

        assert expected_data == data_from_database

        result.close()

        # Assert that the table was properly created
        table_def = connection.execute(f'Describe {table_name}')

        assert [column in table_def.fetchall() for column in
                [('name', 'string', None), ('role', 'string', None), ('age', 'int', None), ('team', 'string', None),
                 ('# Partition Information', '', ''), ('# col_name', 'data_type', 'comment'), ('team', 'string', None)]]

        table_def.close()
    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('pre_created_table', [True, False])
@pytest.mark.parametrize('partition_columns', ['Empty',
                                               'Incorrect',
                                               'Correct',
                                               'Multiple',
                                               'MultipleWithSpaces',
                                               'MultipleWithWrongColumns',
                                               'Wildcard'])
def test_partition_table_without_els(sdc_builder, sdc_executor, deltalake, aws, pre_created_table, partition_columns):
    """Test for Databricks Delta lake target stage. Data is inserted into Delta Lake using the pipeline.

    Each pipeline look like:
        dev_raw_data_source  >> databricks_deltalake
    """
    table_name = f'stf_{get_random_string()}'

    partition_information = []
    if partition_columns == 'Correct' and not pre_created_table:
        partition_information.append({'tableName': table_name, 'columnName': 'genre'})
    elif partition_columns == 'Incorrect':
        partition_information.append({'tableName': 'Not the table name', 'columnName': 'genre'})
    elif partition_columns == 'Multiple':
        partition_information.append({'tableName': table_name, 'columnName': 'genre,title'})
    elif partition_columns == 'MultipleWithSpaces':
        partition_information.append({'tableName': table_name, 'columnName': 'genre, title'})
    elif partition_columns == 'MultipleWithWrongColumns':
        partition_information.append({'tableName': table_name, 'columnName': 'genre, Alex'})
    elif partition_columns == 'Wildcard':
        partition_information.append({'tableName': '*', 'columnName': 'genre'})

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
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        partition_table=True,
                                        partition_columns=partition_information)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    sdc_executor.add_pipeline(pipeline)
    connection = deltalake.connect_engine(engine)
    try:
        if partition_columns in ['Correct', 'Multiple', 'MultipleWithSpaces']:
            if pre_created_table:
                if partition_columns == 'Correct':
                    create_table_query = f"CREATE TABLE {table_name} (title STRING, author STRING, genre STRING, " \
                                         f"publisher STRING) USING DELTA PARTITIONED BY (genre)"
                elif partition_columns in ['Multiple', 'MultipleWithSpaces']:
                    create_table_query = f"CREATE TABLE {table_name} (title STRING, author STRING, genre STRING, " \
                                         f"publisher STRING) USING DELTA PARTITIONED BY (genre, title)"
                connection.execute(create_table_query)

            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

            # Assert data from deltalake table is same as what was input.
            result = connection.execute(f'select * from {table_name}')
            data_from_database = sorted(result.fetchall())

            expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]
            assert len(data_from_database) == len(expected_data)
            assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                     for record in data_from_database]

            # Assert that the table was properly created
            table_def = connection.execute(f'Describe {table_name}')
            if partition_columns == 'Correct':
                assert [column in table_def.fetchall() for column in
                        [('title', 'string', None),
                         ('author', 'string', None),
                         ('genre', 'string', None),
                         ('publisher', 'string', None),
                         ('# Partition Information', '', ''),
                         ('# col_name', 'data_type', 'comment'),
                         ('genre', 'string', None)]]
            elif partition_columns in ['Multiple', 'MultipleWithSpaces']:
                assert [column in table_def.fetchall() for column in
                        [('title', 'string', None),
                         ('author', 'string', None),
                         ('genre', 'string', None),
                         ('publisher', 'string', None),
                         ('# Partition Information', '', ''),
                         ('# col_name', 'data_type', 'comment'),
                         ('genre', 'string', None),
                         ('title', 'string', None)]]

            table_def.close()
        elif partition_columns in ['Incorrect', 'Wildcard']:
            try:
                sdc_executor.start_pipeline(pipeline)
            except StartError as e:
                assert 'DELTA_LAKE_41' in str(e.args[0])
        elif partition_columns == 'Empty':
            try:
                sdc_executor.start_pipeline(pipeline)
            except StartError as e:
                assert 'DELTA_LAKE_40' in str(e.args[0])
        elif partition_columns == 'MultipleWithWrongColumns':
            try:
                sdc_executor.start_pipeline(pipeline)
            except RunError as e:
                assert 'DELTA_LAKE_42' in str(e.args[0])

    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)

@azure('deltalake')
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('pre_created_table', [True, False])
@pytest.mark.parametrize('partition_columns', ['Correct',
                                               'Multiple',
                                               'MultipleWithSpaces'])
def test_partition_table_with_unity_catalog(sdc_builder, sdc_executor, deltalake, azure, pre_created_table,
                                                  partition_columns):
    """Test for Databricks Delta lake target stage. Data is inserted into Delta Lake using the pipeline.

    Each pipeline look like:
        dev_raw_data_source  >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    target_catalog = deltalake.workspace_catalog_name
    target_schema = deltalake.workspace_schema_name
    table_name = f'{target_catalog}.{target_schema}.stf_unity_catalog_{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    partition_information = []
    if partition_columns == 'Correct' and not pre_created_table:
        partition_information.append({'tableName': table_name, 'columnName': 'genre'})
    elif partition_columns == 'Multiple':
        partition_information.append({'tableName': table_name, 'columnName': 'genre,title'})
    elif partition_columns == 'MultipleWithSpaces':
        partition_information.append({'tableName': table_name, 'columnName': 'genre, title'})

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # ADLS gen2 storage destination
    files_prefix = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='ADLS_GEN2',
                                        stage_file_prefix=files_prefix,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True)

    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        partition_table=True,
                                        table_location_path=uri_external_location,
                                        partition_columns=partition_information)

    deltalake = set_sdc_stage_config(deltalake, 'config.dataLakeGen2Stage.connection.authMethod', 'SHARED_KEY')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, azure)
    sdc_executor.add_pipeline(pipeline)
    connection = deltalake.connect_engine(engine)
    try:
        if partition_columns in ['Correct', 'Multiple', 'MultipleWithSpaces']:
            if pre_created_table:
                if partition_columns == 'Correct':
                    create_table_query = f"CREATE TABLE {table_name} (title STRING, author STRING, genre STRING, " \
                                         f"publisher STRING) USING DELTA PARTITIONED BY (genre)"
                elif partition_columns in ['Multiple', 'MultipleWithSpaces']:
                    create_table_query = f"CREATE TABLE {table_name} (title STRING, author STRING, genre STRING, " \
                                         f"publisher STRING) USING DELTA PARTITIONED BY (genre, title)"
                connection.execute(create_table_query)

            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

            # Assert data from deltalake table is same as what was input.
            result = connection.execute(f'select * from {table_name}')
            data_from_database = sorted(result.fetchall())

            expected_data = [tuple(v for v in d.values()) for d in ROWS_IN_DATABASE]
            assert len(data_from_database) == len(expected_data)
            assert expected_data == [(record['title'], record['author'], record['genre'], record['publisher'])
                                     for record in data_from_database]

            # Assert that the table was properly created
            table_def = connection.execute(f'Describe {table_name}')
            if partition_columns == 'Correct':
                assert [column in table_def.fetchall() for column in
                        [('title', 'string', None),
                         ('author', 'string', None),
                         ('genre', 'string', None),
                         ('publisher', 'string', None),
                         ('# Partition Information', '', ''),
                         ('# col_name', 'data_type', 'comment'),
                         ('genre', 'string', None)]]
            elif partition_columns in ['Multiple', 'MultipleWithSpaces']:
                assert [column in table_def.fetchall() for column in
                        [('title', 'string', None),
                         ('author', 'string', None),
                         ('genre', 'string', None),
                         ('publisher', 'string', None),
                         ('# Partition Information', '', ''),
                         ('# col_name', 'data_type', 'comment'),
                         ('genre', 'string', None),
                         ('title', 'string', None)]]

            table_def.close()

    finally:
        _clean_up_databricks(deltalake, table_name)

@azure('deltalake')
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('pre_created_table', [True, False])
@pytest.mark.parametrize('partition_columns', ['Empty',
                                               'Incorrect',
                                               'MultipleWithWrongColumns',
                                               'Wildcard'])
def test_partition_table_with_unity_catalog_error(sdc_builder, sdc_executor, deltalake, azure,
                                                          pre_created_table, partition_columns):
    """Test for Databricks Delta lake target stage. Data is inserted into Delta Lake using the pipeline.

    Each pipeline look like:
        dev_raw_data_source  >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    target_catalog = deltalake.workspace_catalog_name
    target_schema = deltalake.workspace_schema_name
    table_name = f'{target_catalog}.{target_schema}.stf_unity_catalog_{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    partition_information = []
    if partition_columns == 'Incorrect':
        partition_information.append({'tableName': 'Not the table name', 'columnName': 'genre'})
    elif partition_columns == 'MultipleWithWrongColumns':
        partition_information.append({'tableName': table_name, 'columnName': 'genre, Alex'})
    elif partition_columns == 'Wildcard':
        partition_information.append({'tableName': '*', 'columnName': 'genre'})

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # ADLS gen2 storage destination
    files_prefix = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='ADLS_GEN2',
                                        stage_file_prefix=files_prefix,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True)

    databricks_deltalake.set_attributes(table_name=table_name,
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        partition_table=True,
                                        table_location_path=uri_external_location,
                                        partition_columns=partition_information)

    deltalake = set_sdc_stage_config(deltalake, 'config.dataLakeGen2Stage.connection.authMethod', 'SHARED_KEY')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        if partition_columns in ['Incorrect', 'Wildcard']:
            try:
                sdc_executor.start_pipeline(pipeline)
            except (StartError, StartingError) as e:
                assert 'DELTA_LAKE_41' in str(e.args[0])
        elif partition_columns == 'Empty':
            try:
                sdc_executor.start_pipeline(pipeline)
            except (StartError, StartingError) as e:
                assert 'DELTA_LAKE_40' in str(e.args[0])
        elif partition_columns == 'MultipleWithWrongColumns':
            try:
                sdc_executor.start_pipeline(pipeline)
            except (RunError, RunningError) as e:
                assert 'DELTA_LAKE_42' in str(e.args[0])
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('partition_columns', ['EL', 'Wildcard', 'Override', 'Empty', 'MissingColumn'])
def test_partition_table_with_el(sdc_builder, sdc_executor, deltalake, aws, partition_columns):
    """Test for Databricks Delta lake target stage. Data is inserted into Delta Lake using the pipeline.
    Two pipelines are run, the second one with a newer column.
    The expected behavior is to have six rows.

    Each pipeline look like:
        dev_raw_data_source  >> databricks_deltalake
    """
    table_name = f'stf_table_{get_random_string(string.ascii_lowercase, 5)}'
    data = []

    engine = deltalake.engine
    for i in range(50):
        data.append({'table': f'{table_name}_{random.randint(1, 2)}', 'id': i, 'name': get_random_string()})

    raw_data = '\n'.join((json.dumps(row) for row in data))

    partition_information = []
    if partition_columns == 'EL':
        partition_information.append({'tableName': "record:value('/table')", 'columnName': 'id'})
    elif partition_columns == 'Wildcard':
        partition_information.append({'tableName': "*", 'columnName': 'id'})
    elif partition_columns == 'Override':
        partition_information.append({'tableName': "record:value('/table')", 'columnName': 'id'})
        partition_information.append({'tableName': f'{table_name}_2', 'columnName': 'name'})
    elif partition_columns == 'MissingColumn':
        partition_information.append({'tableName': "record:value('/table')", 'columnName': 'random'})

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    # AWS S3 destination
    s3_key = f'stf-deltalake/{get_random_string()}'

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='AWS_S3',
                                        stage_file_prefix=s3_key,
                                        table_name="${record:value('/table')}",
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        partition_table=True,
                                        partition_columns=partition_information)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    sdc_executor.add_pipeline(pipeline)
    connection = deltalake.connect_engine(engine)
    try:
        if partition_columns in ['EL', 'Wildcard', 'Override']:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

            # First table.
            result = connection.execute(f'select * from {table_name}_1')
            data_from_database = sorted(result.fetchall())

            expected_data = [(row['table'], row['id'], row['name']) for row in
                             list(filter(lambda x: x['table'] == f'{table_name}_1', data))]
            assert len(data_from_database) == len(expected_data)
            assert expected_data == data_from_database

            # Assert that the table was properly created
            table_def = connection.execute(f'Describe {table_name}_1')
            assert [column in table_def.fetchall() for column in
                    [('table', 'string', None), ('id', 'int', None), ('name', 'string', None),
                     ('# Partition Information', '', ''), ('# col_name', 'data_type', 'comment'),
                     ('id', 'int', None)]]

            table_def.close()

            # Second table.
            result = connection.execute(f'select * from {table_name}_2')
            data_from_database = sorted(result.fetchall())

            expected_data = [(row['table'], row['id'], row['name']) for row in
                             list(filter(lambda x: x['table'] == f'{table_name}_2', data))]
            assert len(data_from_database) == len(expected_data)
            assert expected_data == data_from_database

            # Assert that the table was properly created
            table_def = connection.execute(f'Describe {table_name}_2')

            if partition_columns in ['EL', 'Wildcard']:
                assert [column in table_def.fetchall() for column in
                        [('table', 'string', None),
                         ('id', 'int', None),
                         ('name', 'string', None),
                         ('# Partition Information', '', ''),
                         ('# col_name', 'data_type', 'comment'),
                         ('id', 'int', None)]]
            else:
                assert [column in table_def.fetchall() for column in
                        [('table', 'string', None),
                         ('id', 'int', None),
                         ('name', 'string', None),
                         ('# Partition Information', '', ''),
                         ('# col_name', 'data_type', 'comment'),
                         ('name', 'string', None)]]

            table_def.close()
        elif partition_columns == 'Empty':
            try:
                sdc_executor.start_pipeline(pipeline)
            except StartError as e:
                assert 'DELTA_LAKE_40' in str(e.args[0])
        elif partition_columns == 'MissingColumn':
            try:
                sdc_executor.start_pipeline(pipeline)
            except RunError as e:
                assert 'DELTA_LAKE_42' in str(e.args[0])

    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
def test_table_location_path_with_partition(sdc_builder, sdc_executor, deltalake, aws):
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
                                        purge_stage_file_after_ingesting=True,
                                        enable_data_drift=True,
                                        auto_create_table=True,
                                        partition_table=True,
                                        partition_columns=[{'tableName': table_name, 'columnName': 'genre'}])

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

        create_table = str(result.fetchall()[0])

        assert f'dbfs:{table_location}{table_name}' in create_table
        result.close()

        table_def = connection.execute(f'Describe {table_name}')
        assert [column in table_def.fetchall() for column in
                [('title', 'string', None),
                 ('author', 'string', None),
                 ('genre', 'string', None),
                 ('publisher', 'string', None),
                 ('# Partition Information', '', ''),
                 ('# col_name', 'data_type', 'comment'),
                 ('genre', 'string', None)]]
        table_def.close()
    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@pytest.mark.parametrize('staging_format', ['CSV', 'AVRO - IN LINE', 'AVRO - HEADER', 'AVRO - INFER'])
def test_staging_file_formats(sdc_builder, sdc_executor, deltalake, aws, staging_format):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake

    In case of 'AVRO - HEADER' it will include also a schema_generator
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
                                        staging_file_format=staging_format,
                                        enable_data_drift=False,
                                        auto_create_table=False)

    if staging_format == 'AVRO - IN LINE':
        avro_schema = {
            "type": "record",
            "name": "DeltaLake",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "author", "type": "string"},
                {"name": "genre", "type": "string"},
                {"name": "publisher", "type": "string"}
            ]
        }
        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='INLINE',
                                            avro_schema=json.dumps(avro_schema))
    elif staging_format == 'AVRO - HEADER':
        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='HEADER')

    elif staging_format == 'AVRO - INFER':
        databricks_deltalake.set_attributes(staging_file_format='AVRO',
                                            avro_schema_location='INFER')

    if staging_format == 'AVRO - HEADER':
        # Basically if we use in-header schema we need to add a schema generator in the middle
        schema_generator = pipeline_builder.add_stage('Schema Generator')
        schema_generator.schema_name = 'DeltaLake'

        dev_raw_data_source >> schema_generator >> databricks_deltalake
    else:
        dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)
        logger.info(f'Creating table {table_name} ...')
        query = f'create table {table_name} (title string, author string, genre string, publisher string) using DELTA'
        connection.execute(query)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        _clean_up_databricks(deltalake, table_name)


@gcp('gcs')
def test_with_gcs_storage(sdc_builder, sdc_executor, deltalake, gcp):
    """Test for Databricks Delta Lake that uses GCS as storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """

    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) <= MIN_VERSION_CLUSTER:
        pytest.skip('Test with Google Cloud Store will only run against Databricks Cluster Versions >= 6')

    table_name = f'stf_{get_random_string()}'

    engine = deltalake.engine
    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # Databricks Delta lake destination stage
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)
    databricks_deltalake.set_attributes(staging_location='GCS',
                                        project_id=gcp.project_id,
                                        table_name=table_name,
                                        purge_stage_file_after_ingesting=True)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, gcp)
    
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
@pytest.mark.parametrize('tags_size', [5, 20])
@sdc_min_version('5.7.0')
def test_s3_tags(sdc_builder, sdc_executor, deltalake, aws, tags_size):
    """Ensure the library works properly using s3 tags"""

    tags = {}
    for _ in range(tags_size):
        tags[get_random_string()] = get_random_string()
    s3_tags = []
    for key, value in tags.items():
        s3_tags.append({"key": key, "value": value})
    s3_tags = sorted(s3_tags, key=itemgetter('key'))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage("Dev Data Generator")
    databricks_deltalake = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME)

    dev_data_generator.set_attributes(
        records_to_be_generated=1,
        batch_size=1,
        fields_to_generate=[{"type": "LONG_SEQUENCE", "field": "seq"}]
    )

    table_name = f"test_s3_tags_{get_random_string()}"
    s3_key = f"stf-deltalake/{get_random_string()}"
    databricks_deltalake.set_attributes(
        staging_location="AWS_S3",
        stage_file_prefix=s3_key,
        table_name=table_name,
        tags=s3_tags)

    dev_data_generator >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    databricks_deltalake.purge_stage_file_after_ingesting = False  # configure_for_environment() sets it to True!
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection = deltalake.connect_engine(deltalake.engine)
        connection.execute(
            f"create table {table_name} (seq INT) using delta location '/deltalake/{table_name}_{get_random_string()}'"
        )

        if len(tags) > 10:
            with pytest.raises(Exception) as error:
                sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
            assert "AWS_02" in error.value.message, f'Expected a AWS_02 error, got "{error.value.message}" instead'
        else:
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

            s3_bucket_objects = aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)
            assert 'Contents' in s3_bucket_objects, f'Contents not found in response: {s3_bucket_objects}'
            keys = [k['Key'] for k in s3_bucket_objects['Contents']]
            assert any(keys), f'keys not found in Contents response: {s3_bucket_objects["Contents"]}'
            for key in keys:
                response = aws.s3.get_object_tagging(Bucket=aws.s3_bucket_name, Key=key)
                assert 'TagSet' in response, f'TagSet not found in response: {response}'
                response_tags = sorted(response['TagSet'], key=itemgetter('Key'))
                assert len(response_tags) == len(s3_tags), "number of tags differ!"
                diff = [(x, y) for x, y in zip(s3_tags, response_tags) if x['key'] != y['Key'] or x['value'] != y['Value']]
                assert not any(diff), f'tags do not match!'

    finally:
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)
        try:
            deltalake.delete_table(table_name)
        except Exception as ex:
            logger.error(f"Error encountered while deleting Databricks Delta Lake table = {table_name} as {ex}")


@aws('s3')
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('pre_create_table', [True, False])
def test_unity_catalog_with_el_expression_table_name(sdc_builder, sdc_executor, deltalake, aws, pre_create_table):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    target_catalog = deltalake.workspace_catalog_name
    target_schema = deltalake.workspace_schema_name
    table_name_1 = f'{target_catalog}.{target_schema}.stf_unity_catalog_{get_random_string()}'
    table_name_2 = f'{target_catalog}.{target_schema}.stf_unity_catalog_{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    rows_data = [{"title": f'Bicycles are for the summer', "author": "Pepito", "genre": "Comedy",
                  "publisher": "HarperCollins Publishers", "table_name": f"{table_name_1}"},
                 {"title": f'Bicycles are for the summer', "author": "Pepito", "genre": "Comedy",
                  "publisher": "HarperCollins Publishers", "table_name": f"{table_name_2}"},
                 ]

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
                                        table_name="${record:value('/table_name')}",
                                        table_location_path=uri_external_location,
                                        purge_stage_file_after_ingesting=True,
                                        column_separator=';')
    if pre_create_table:
        databricks_deltalake.set_attributes(auto_create_table=False,
                                            enable_data_drift=False)
    else:
        databricks_deltalake.set_attributes(auto_create_table=True,
                                            enable_data_drift=True)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        connection = deltalake.connect_engine(engine)
        if pre_create_table:
            logger.info(f'Creating table {table_name_1} ...')
            query = f'create table {table_name_1} (title string, author string, genre string, publisher string) using ' \
                    f"DELTA LOCATION '{uri_external_location}/{table_name_1}'"
            connection.execute(query)
            logger.info(f'Creating table {table_name_2} ...')
            query = f'create table {table_name_2} (title string, author string, genre string, publisher string)' \
                    f"using DELTA LOCATION '{uri_external_location}/{table_name_2}'"
            connection.execute(query)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select * from {table_name_1}')
        data_from_database_1 = sorted(result.fetchall())
        result = connection.execute(f'select * from {table_name_2}')
        data_from_database_2 = sorted(result.fetchall())

        assert len(data_from_database_1) == 1
        assert [(data['title'], data['author'], data['genre'], data['publisher']) for data in data_from_database_1] == \
               [(rows_data[0]['title'], rows_data[0]['author'], rows_data[0]['genre'], rows_data[0]['publisher'])]

        assert len(data_from_database_2) == 1
        assert [(data['title'], data['author'], data['genre'], data['publisher']) for data in data_from_database_2] \
               == [(rows_data[1]['title'], rows_data[1]['author'], rows_data[1]['genre'], rows_data[1]['publisher'])]

        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0

    finally:
        _clean_up_databricks(deltalake, table_name_1)
        _clean_up_databricks(deltalake, table_name_2)


@aws('s3')
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('pre_create_table', [True, False])
def test_unity_catalog_with_constant_parameter_location(sdc_builder, sdc_executor, deltalake, aws, pre_create_table):
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
                  "publisher": "HarperCollins Publishers"},
                 {"title": f"Harry potter and the philosopher's stone", "author": "JK.Rowling", "genre": "Fantasy",
                  "publisher": "HarperCollins Publishers"},
                 ]

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
                                        table_location_path="${unity_catalog_location}",
                                        purge_stage_file_after_ingesting=True,
                                        column_separator=';')
    if pre_create_table:
        databricks_deltalake.set_attributes(auto_create_table=False,
                                            enable_data_drift=False)
    else:
        databricks_deltalake.set_attributes(auto_create_table=True,
                                            enable_data_drift=True)

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)
    pipeline.add_parameters(unity_catalog_location=uri_external_location)

    try:
        connection = deltalake.connect_engine(engine)
        if pre_create_table:
            logger.info(f'Creating table {table_name} ...')
            query = f'create table {table_name} (title string, author string, genre string, publisher string) using ' \
                    f"DELTA LOCATION '{uri_external_location}/{table_name}'"
            connection.execute(query)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)

        # Assert data from deltalake table is same as what was input.
        result = connection.execute(f'select * from {table_name}')
        data_from_database = sorted(result.fetchall())

        assert len(data_from_database) == len(rows_data)
        assert [(data['title'], data['author'], data['genre'], data['publisher']) for data in data_from_database] == \
               [(row['title'], row['author'], row['genre'], row['publisher']) for row in rows_data]

        result.close()

        # Assert that we actually purged the staged file
        assert aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] == 0

    finally:
        _clean_up_databricks(deltalake, table_name)


@aws('s3')
@sdc_min_version('5.7.0')
def test_unity_catalog_with_incorrect_table_name(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    table_name = f'stf_sdc_{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    rows_data = [{"title": f'Bicycles are for the summer', "author": "Pepito", "genre": "Comedy",
                  "publisher": "HarperCollins Publishers"},
                 {"title": f"Harry potter and the philosopher's stone", "author": "JK.Rowling", "genre": "Fantasy",
                  "publisher": "HarperCollins Publishers"},
                 ]

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
                                        table_location_path=uri_external_location,
                                        purge_stage_file_after_ingesting=True,
                                        auto_create_table=True,
                                        enable_data_drift=True,
                                        column_separator=';')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
    except StartError as e:
        assert 'DELTA_LAKE_46' in str(e.args[0])


@aws('s3')
@sdc_min_version('5.7.0')
def test_unity_catalog_with_el_expression_table_name_incorrect(sdc_builder, sdc_executor, deltalake, aws):
    """Test for Databricks Delta Lake with AWS S3 storage.

    The pipeline looks like this:
        dev_raw_data_source >> databricks_deltalake
    """
    if int(re.findall(REGEX_EXPRESSION_DATABRICKS_VERSION, deltalake.cluster_name)[0]) < MIN_VERSION_UNITY_CATALOG:
        pytest.skip('Test only runs against Databricks cluster with Unity Catalog')

    table_name = f'stf_sdc_{get_random_string()}'
    uri_external_location = deltalake.workspace_external_location_path

    rows_data = [{"title": f'Bicycles are for the summer', "author": "Pepito", "genre": "Comedy",
                  "publisher": "HarperCollins Publishers", "table_name": f"{table_name}"},
                 {"title": f"Harry potter and the philosopher's stone", "author": "JK.Rowling", "genre": "Fantasy",
                  "publisher": "HarperCollins Publishers", "table_name": f"{table_name}"}
                 ]

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
                                        table_name="${record:value('/table_name')}",
                                        table_location_path=uri_external_location,
                                        purge_stage_file_after_ingesting=True,
                                        auto_create_table=True,
                                        enable_data_drift=True,
                                        column_separator=';')

    dev_raw_data_source >> databricks_deltalake

    pipeline = pipeline_builder.build().configure_for_environment(deltalake, aws)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=120)
        history = sdc_executor.get_pipeline_history(pipeline)

        assert history.latest.metrics._data['counters']['stage.DatabricksDeltaLake_01.errorRecords.counter']['count']\
               == 2

    finally:
        _clean_up_databricks(deltalake, table_name)
