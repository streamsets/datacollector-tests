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
import pytest

from streamsets.testframework.decorators import stub


@stub
def test_access_key_id(sdc_builder, sdc_executor):
    pass


@stub
def test_bucket(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CREATE_NEW_OBJECT'}])
def test_content(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'delete_original_object': False, 'task': 'COPY_OBJECT'},
                                              {'delete_original_object': True, 'task': 'COPY_OBJECT'}])
def test_delete_original_object(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'OTHER'}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'COPY_OBJECT'}])
def test_new_object_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_object(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_host(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_port(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_user(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'AP_NORTHEAST_1'},
                                              {'region': 'AP_NORTHEAST_2'},
                                              {'region': 'AP_NORTHEAST_3'},
                                              {'region': 'AP_SOUTHEAST_1'},
                                              {'region': 'AP_SOUTHEAST_2'},
                                              {'region': 'AP_SOUTH_1'},
                                              {'region': 'CA_CENTRAL_1'},
                                              {'region': 'CN_NORTHWEST_1'},
                                              {'region': 'CN_NORTH_1'},
                                              {'region': 'EU_CENTRAL_1'},
                                              {'region': 'EU_WEST_1'},
                                              {'region': 'EU_WEST_2'},
                                              {'region': 'EU_WEST_3'},
                                              {'region': 'OTHER'},
                                              {'region': 'SA_EAST_1'},
                                              {'region': 'US_EAST_1'},
                                              {'region': 'US_EAST_2'},
                                              {'region': 'US_GOV_WEST_1'},
                                              {'region': 'US_WEST_1'},
                                              {'region': 'US_WEST_2'}])
def test_region(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_retry_count(sdc_builder, sdc_executor):
    pass


@stub
def test_secret_access_key(sdc_builder, sdc_executor):
    pass


@stub
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CHANGE_EXISTING_OBJECT'}])
def test_tags(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CHANGE_EXISTING_OBJECT'},
                                              {'task': 'COPY_OBJECT'},
                                              {'task': 'CREATE_NEW_OBJECT'}])
def test_task(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass

