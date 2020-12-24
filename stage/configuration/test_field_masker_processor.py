# Copyright 2020 StreamSets Inc.
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

import json

import pytest
from streamsets.testframework.decorators import stub


def test_field_mask_configs(sdc_builder, sdc_executor):
    """Test field mask configs.

     Input           | Mask Type          | Output
    -----------------|--------------------|------------------
     donKey          | Fixed length       | xxxxxxxxxx
     donKey          | Variable length    | xxxxxx
     617-567-8888    | Custom             | 617-xxx-xxxx
     123-45-6789     | Regular Expression | xxxx45xxxxx
    ---------------------------------------------------------
    """
    DATA = dict(password1='donKey', password2='donKey', phoneNumber='617-567-8888', ssn='123-45-6789')
    EXPECTED_MASKED_DATA = dict(password1='xxxxxxxxxx', password2='xxxxxx',
                                phoneNumber='617-xxx-xxxx', ssn='xxxx45xxxxx')

    field_mask_configs = [{'fields': ['/password1'],
                           'maskType': 'FIXED_LENGTH',
                           'regex': '(.*)',
                           'groupsToShow': '1'},
                          {'fields': ['/password2'],
                           'maskType': 'VARIABLE_LENGTH',
                           'regex': '(.*)',
                           'groupsToShow': '1'},
                          {'fields': ['/phoneNumber'],
                           'maskType': 'CUSTOM',
                           'regex': '(.*)',
                           'groupsToShow': '1',
                           'mask': '###-xxx-xxxx'},
                          {'fields': ['/ssn'],
                           'maskType': 'REGEX',
                           'regex': '([0-9]{3})-([0-9]{2})-([0-9]{4})',
                           'groupsToShow': '2'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(
        data_format='JSON', raw_data=json.dumps(DATA), stop_after_first_batch=True)
    field_masker = pipeline_builder.add_stage('Field Masker').set_attributes(field_mask_configs=field_mask_configs)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_masker >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    record = wiretap.output_records[0]
    assert record.field == EXPECTED_MASKED_DATA


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

