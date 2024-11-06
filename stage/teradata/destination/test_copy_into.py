# Copyright 2024 StreamSets Inc.
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

import pytest
from streamsets.testframework.markers import aws, sdc_min_version

from stage.utils.common import cleanup
from . import (
    pytestmark,
    stage_name,
    teradata_manager,
    ALL_STAGING_LOCATIONS,
    LOCAL_STAGING_LOCATION
)
from stage.utils.data_loading_stages import (
    SIMPLE_ROWS,
    DUPLICATE_COLUMN_ROWS,
    data_loading_pipeline_handler_creator
)


@pytest.mark.parametrize('staging_location', ALL_STAGING_LOCATIONS)
def test_copy_into(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                   data_loading_pipeline_handler_creator, staging_location):
    """
        Assert that some rows are properly loaded
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': False,
        'enable_data_drift': False
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_copy_into_duplicate_rows(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                  data_loading_pipeline_handler_creator, staging_location):
    """
        Assert that when inserting duplicate rows that are not primary keys, there are no errors
    """
    test_data = DUPLICATE_COLUMN_ROWS
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': False,
        'enable_data_drift': False
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == DUPLICATE_COLUMN_ROWS


@aws('s3')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('staging_location', ['AWS_S3'])
@pytest.mark.parametrize('specify_region', [
    None,
    'use_region',
    'use_custom_region',
    'use_regional_endpoint',
    'use_regional_vpc_endpoint',
    'use_custom_endpoint_and_signing_region',
    'use_custom_endpoint_and_custom_signing_region'
])
def test_aws_staging_region_configuration(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                          data_loading_pipeline_handler_creator, aws, staging_location, specify_region):
    """
    Tes3 that the Teradata Destination can use assume role and specify region when using AWS storage location.

    The pipeline created looks like:
        dev_raw_data_source  >> teradata_destination
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': False,
        'enable_data_drift': False
    }

    if specify_region == 'use_region':
        teradata_attributes.update({
            'region_definition_for_s3': "SPECIFY_REGION",
            'region_for_s3': aws.formatted_region
        })
    elif specify_region == 'use_custom_region':
        teradata_attributes.update({
            'region_definition_for_s3': "SPECIFY_REGION",
            'region_for_s3': "OTHER",
            'custom_region_for_s3': aws.region
        })
    elif specify_region == 'use_regional_endpoint':
        teradata_attributes.update({
            'region_definition_for_s3': "SPECIFY_REGIONAL_ENDPOINT",
            'regional_endpoint_for_s3': f"s3.{aws.region}.amazonaws.com"
        })
    elif specify_region == 'use_regional_vpc_endpoint':
        teradata_attributes.update({
            'region_definition_for_s3': "SPECIFY_REGIONAL_ENDPOINT",
            'regional_endpoint_for_s3': aws.vpc_endpoint
        })
    elif specify_region == 'use_custom_endpoint_and_signing_region':
        teradata_attributes.update({
            'region_definition_for_s3': "SPECIFY_NON_REGIONAL_ENDPOINT",
            'custom_endpoint_for_s3': aws.vpc_endpoint,
            'signing_region_for_s3': aws.formatted_region
        })
    elif specify_region == 'use_custom_endpoint_and_custom_signing_region':
        teradata_attributes.update({
            'region_definition_for_s3': "SPECIFY_NON_REGIONAL_ENDPOINT",
            'custom_endpoint_for_s3': aws.vpc_endpoint,
            'signing_region_for_s3': "OTHER",
            'custom_signing_region_for_s3': aws.region
        })
    else:
        teradata_attributes.update({
            'region_definition_for_s3': "NOT_SPECIFIED"
        })

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data


@aws('s3')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('staging_location', ['AWS_S3'])
@pytest.mark.parametrize('specify_sts_region', [
    None,
    'use_region',
    'use_custom_region',
    'use_regional_endpoint',
    'use_regional_vpc_endpoint',
    'use_custom_endpoint_and_signing_region',
    'use_custom_endpoint_and_custom_signing_region'
])
def test_aws_staging_with_assume_role(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager,
                                      stage_name, data_loading_pipeline_handler_creator, aws,
                                      staging_location, specify_sts_region):
    """
    Tes3 that the Teradata Destination can use assume role and specify region when using AWS storage location.

    The pipeline created looks like:
        dev_raw_data_source  >> teradata_destination
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': False,
        'enable_data_drift': False,
        'assume_role': True,
        'role_arn': aws.iam_role
    }

    if specify_sts_region == 'use_region':
        teradata_attributes.update({
            'region_definition_for_sts': "SPECIFY_REGION",
            'region_for_sts': aws.formatted_region
        })
    elif specify_sts_region == 'use_custom_region':
        teradata_attributes.update({
            'region_definition_for_sts': "SPECIFY_REGION",
            'region_for_sts': "OTHER",
            'custom_region_for_sts': aws.region
        })
    elif specify_sts_region == 'use_regional_endpoint':
        teradata_attributes.update({
            'region_definition_for_sts': "SPECIFY_REGIONAL_ENDPOINT",
            'regional_endpoint_for_sts': f"sts.{aws.region}.amazonaws.com"
        })
    elif specify_sts_region == 'use_regional_vpc_endpoint':
        teradata_attributes.update({
            'region_definition_for_sts': "SPECIFY_REGIONAL_ENDPOINT",
            'regional_endpoint_for_sts': aws.sts_vpc_endpoint
        })
    elif specify_sts_region == 'use_custom_endpoint_and_signing_region':
        teradata_attributes.update({
            'region_definition_for_sts': "SPECIFY_NON_REGIONAL_ENDPOINT",
            'custom_endpoint_for_sts': aws.sts_vpc_endpoint,
            'signing_region_for_sts': aws.formatted_region
        })
    elif specify_sts_region == 'use_custom_endpoint_and_custom_signing_region':
        teradata_attributes.update({
            'region_definition_for_sts': "SPECIFY_NON_REGIONAL_ENDPOINT",
            'custom_endpoint_for_sts': aws.sts_vpc_endpoint,
            'signing_region_for_sts': "OTHER",
            'custom_signing_region_for_sts': aws.region
        })
    else:
        teradata_attributes.update({
            'region_definition_for_sts': "NOT_SPECIFIED"
        })

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data
