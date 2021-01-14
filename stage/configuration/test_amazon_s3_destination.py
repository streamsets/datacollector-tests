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
@pytest.mark.parametrize('stage_attributes', [{'server_side_encryption_option': 'KMS',
                                               'use_server_side_encryption': True}])
def test_aws_kms_key_arn(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_bucket(sdc_builder, sdc_executor):
    pass


@stub
def test_common_prefix(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compress_with_gzip': False}, {'compress_with_gzip': True}])
def test_compress_with_gzip(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'server_side_encryption_option': 'CUSTOMER',
                                               'use_server_side_encryption': True}])
def test_customer_encryption_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'server_side_encryption_option': 'CUSTOMER',
                                               'use_server_side_encryption': True}])
def test_customer_encryption_key_md5(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
def test_delimiter(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'server_side_encryption_option': 'KMS',
                                               'use_server_side_encryption': True}])
def test_encryption_context(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'OTHER'}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_minimum_upload_part_size(sdc_builder, sdc_executor):
    pass


@stub
def test_multipart_upload_threshold(sdc_builder, sdc_executor):
    pass


@stub
def test_object_name_prefix(sdc_builder, sdc_executor):
    pass


@stub
def test_object_name_suffix(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_partition_prefix(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'server_side_encryption_option': 'CUSTOMER',
                                               'use_server_side_encryption': True},
                                              {'server_side_encryption_option': 'KMS',
                                               'use_server_side_encryption': True},
                                              {'server_side_encryption_option': 'NONE',
                                               'use_server_side_encryption': True},
                                              {'server_side_encryption_option': 'S3',
                                               'use_server_side_encryption': True}])
def test_server_side_encryption_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_thread_pool_size_for_parallel_uploads(sdc_builder, sdc_executor):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_server_side_encryption': False},
                                              {'use_server_side_encryption': True}])
def test_use_server_side_encryption(sdc_builder, sdc_executor, stage_attributes):
    pass

