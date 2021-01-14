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
def test_buffer_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'windows_log_to_read_from': 'Custom'}])
def test_custom_log_name(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'reader_api_type': 'WINDOWS_EVENT_LOG'}])
def test_maximum_wait_time_in_secs(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'populate_raw_event_xml': 'ALWAYS',
                                               'reader_api_type': 'WINDOWS_EVENT_LOG'},
                                              {'populate_raw_event_xml': 'ON_ERROR',
                                               'reader_api_type': 'WINDOWS_EVENT_LOG'}])
def test_populate_raw_event_xml(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'read_mode': 'ALL'}, {'read_mode': 'NEW'}])
def test_read_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'reader_api_type': 'EVENT_LOGGING'},
                                              {'reader_api_type': 'WINDOWS_EVENT_LOG'}])
def test_reader_api_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'reader_api_type': 'WINDOWS_EVENT_LOG', 'subscription_mode': 'PULL'},
                                              {'reader_api_type': 'WINDOWS_EVENT_LOG', 'subscription_mode': 'PUSH'}])
def test_subscription_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'windows_log_to_read_from': 'Application'},
                                              {'windows_log_to_read_from': 'Custom'},
                                              {'windows_log_to_read_from': 'Security'},
                                              {'windows_log_to_read_from': 'System'}])
def test_windows_log_to_read_from(sdc_builder, sdc_executor, stage_attributes):
    pass

