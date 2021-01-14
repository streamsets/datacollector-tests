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
def test_connection_timeout_in_sec(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': False}, {'enable_local_caching': True}])
def test_enable_local_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_ACCESS'},
                                              {'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_WRITE'}])
def test_eviction_policy_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_expiration_time(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_lookup_parameters(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_maximum_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BATCH'}, {'mode': 'RECORD'}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
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
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True, 'time_unit': 'DAYS'},
                                              {'enable_local_caching': True, 'time_unit': 'HOURS'},
                                              {'enable_local_caching': True, 'time_unit': 'MICROSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MILLISECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MINUTES'},
                                              {'enable_local_caching': True, 'time_unit': 'NANOSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'SECONDS'}])
def test_time_unit(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_uri(sdc_builder, sdc_executor):
    pass

