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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'COLLECTD'}])
def test_auth_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'convert_hi_res_time_and_interval': False, 'data_format': 'COLLECTD'},
                                              {'convert_hi_res_time_and_interval': True, 'data_format': 'COLLECTD'}])
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'COLLECTD'},
                                              {'data_format': 'NETFLOW'},
                                              {'data_format': 'RAW_DATA'},
                                              {'data_format': 'SYSLOG'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'RAW_DATA'}])
def test_data_separator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'COLLECTD', 'exclude_interval': False},
                                              {'data_format': 'COLLECTD', 'exclude_interval': True}])
def test_exclude_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_size_in_messages(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'}])
def test_max_templates_in_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'RAW_DATA', 'multiple_values_behavior': 'ALL_AS_LIST'},
                                              {'data_format': 'RAW_DATA', 'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'data_format': 'RAW_DATA',
                                               'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_native_transports_in_epoll': True}])
def test_number_of_receiver_threads(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_number_of_worker_threads(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'RAW_DATA'}])
def test_output_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_packet_queue_size(sdc_builder, sdc_executor):
    pass


@stub
def test_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'RAW_DATA', 'raw_data_mode': 'BINARY'},
                                              {'data_format': 'RAW_DATA', 'raw_data_mode': 'CHARACTER'}])
def test_raw_data_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW', 'record_generation_mode': 'INTERPRETED_ONLY'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED'},
                                              {'data_format': 'NETFLOW', 'record_generation_mode': 'RAW_ONLY'}])
def test_record_generation_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'}])
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'COLLECTD'}])
def test_typesdb_file_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_native_transports_in_epoll': False},
                                              {'use_native_transports_in_epoll': True}])
def test_use_native_transports_in_epoll(sdc_builder, sdc_executor, stage_attributes):
    pass

