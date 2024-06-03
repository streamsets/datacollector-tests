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
from streamsets.testframework.utils import get_random_string

from . import (
    pytestmark,
    sdc_min_version,
    stage_name,
    AWS_S3_ENCRYPTION_OPTIONS,
    teradata_manager,
)
from stage.utils.common import cleanup
from stage.utils.data_loading_stages import (
    SIMPLE_ROWS,
    data_loading_pipeline_handler_creator
)

def test_purge_file():
    """
        Assert that Purge File works as expected.
    """
    pytest.skip('We do not have yet the ability to gather staging file information. The way of accessing each '
                'staging location should be different, depending on the system. Getting to acknowledge if those files '
                'were removed or not might not be straightforward, as we do not expose the information of the files '
                'created.')

@sdc_min_version('5.11.0')
@pytest.mark.parametrize('staging_location', ['AWS_S3'])
@pytest.mark.parametrize('s3_encryption', AWS_S3_ENCRYPTION_OPTIONS)
def test_s3_staging_encryption_option(sdc_builder, sdc_executor, aws, teradata, cleanup, teradata_manager, stage_name,
                                   data_loading_pipeline_handler_creator, staging_location, s3_encryption):
    test_data = SIMPLE_ROWS
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()
    s3_prefix = f'sdc-{get_random_string()}'

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'encryption': s3_encryption,
        'purge_stage_file_after_ingesting': False,
        'stage_file_prefix': s3_prefix
    }

    if s3_encryption == 'KMS':
        teradata_attributes.update(aws_kms_key_arn=aws.kms_key_arn)

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    cdc_evaluator = pipeline_handler.insert_operation_evaluator(['id'])
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> cdc_evaluator >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)
    assert data_from_database == test_data

    s3_object_key = authorization.get_s3_object_key_by_prefix(s3_prefix)

    if s3_encryption == 'KMS':
        assert s3_object_key['ServerSideEncryption'] == 'aws:kms'
        assert s3_object_key['SSEKMSKeyId'] == aws.kms_key_arn
    else:
        assert s3_object_key['ServerSideEncryption'] == 'AES256'