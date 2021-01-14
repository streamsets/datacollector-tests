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
def test_aggregations(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'all_aggregators_event': False}, {'all_aggregators_event': True}])
def test_all_aggregators_event(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'window_type': 'ROLLING'}])
def test_number_of_time_windows_to_remember(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'per_aggregator_events': False}, {'per_aggregator_events': True}])
def test_per_aggregator_events(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'produce_event_record_with_text_field': False},
                                              {'produce_event_record_with_text_field': True}])
def test_produce_event_record_with_text_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'time_window': 'TW_10M', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_10S', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_12H', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_15M', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_1D', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_1H', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_1M', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_20M', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_30M', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_30S', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_5M', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_5S', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_6H', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_8H', 'window_type': 'ROLLING'},
                                              {'time_window': 'TW_10M', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_12H', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_1D', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_1H', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_1M', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_30M', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_5M', 'window_type': 'SLIDING'},
                                              {'time_window': 'TW_6H', 'window_type': 'SLIDING'}])
def test_time_window(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'window_type': 'ROLLING'}, {'window_type': 'SLIDING'}])
def test_window_type(sdc_builder, sdc_executor, stage_attributes):
    pass

