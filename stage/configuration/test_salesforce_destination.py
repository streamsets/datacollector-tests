# Copyright 2021 StreamSets Inc.
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

from streamsets.testframework.decorators import stub

import pytest
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.sdk.exceptions import ValidationError

@salesforce
@sdc_min_version('4.0.0')
def test_api_version(sdc_builder, sdc_executor, salesforce):
    """Verify that error FORCE-51 is thrown when using an API field that is not formatted as a valid Salesforce API.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')

    salesforce_destination.set_attributes(api_version='bad_api')

    dev_raw_data_source >> salesforce_destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.validate_pipeline(pipeline)
        pytest.fail('This point should not be reached')
    except ValidationError as error:
        assert error.issues['issueCount'] == 1
        assert 'FORCE_51' in error.issues['stageIssues']['Salesforce_01'][0]['message']


@stub
def test_auth_endpoint(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_operation': 'DELETE'},
                                              {'default_operation': 'INSERT'},
                                              {'default_operation': 'UNDELETE'},
                                              {'default_operation': 'UPDATE'},
                                              {'default_operation': 'UPSERT'}])
def test_default_operation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_external_id_field(sdc_builder, sdc_executor):
    pass


@stub
def test_field_mapping(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_mutual_authentication': True},
                                              {'keystore_type': 'PKCS12', 'use_mutual_authentication': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_password(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_hostname(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_port(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_realm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': False, 'use_proxy': True},
                                              {'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_requires_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_username(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_sobject_type(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_operation_handling': 'DISCARD'},
                                              {'unsupported_operation_handling': 'SEND_TO_ERROR'},
                                              {'unsupported_operation_handling': 'USE_DEFAULT'}])
def test_unsupported_operation_handling(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_bulk_api': False}, {'use_bulk_api': True}])
def test_use_bulk_api(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': False}, {'use_mutual_authentication': True}])
def test_use_mutual_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_username(sdc_builder, sdc_executor):
    pass

