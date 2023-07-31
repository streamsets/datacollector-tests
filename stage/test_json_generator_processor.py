# Copyright 2023 StreamSets Inc.
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
import logging

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('contact_field', ('address', 'email', 'phone'))
@pytest.mark.parametrize('target_field', ('result', 'newcontact'))
@sdc_min_version('2.7.0.0')
def test_json_generator(sdc_builder, sdc_executor, contact_field, target_field):
    """Test JSON Generator processor. The pipeline would look like:

        dev_raw_data_source >> json_generator >> wiretap
    """
    raw_data = """
    {
      "contact": {
        "name": "Jane Smith",
        "id": "557",
        "address": {
          "home": {
            "state": "NC",
            "zipcode": "27023"
          }
        },
        "email": [
          "abc@example.com",
          "def@example.com"
        ],
        "phone": [
          {
            "label": "home",
            "number": "123-456-7890"
          },
          {
            "label": "mobile",
            "number": "000-000-0000"
          }
        ]
      },
      "newcontact": {
        "address": "",
        "email": "",
        "phone": ""
      },
      "result": {}
    }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    json_generator = pipeline_builder.add_stage('JSON Generator', type='processor')
    json_generator.set_attributes(field_to_serialize=f'/contact/{contact_field}',
                                  target_field=f'/{target_field}/{contact_field}')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_generator >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        new_value = wiretap.output_records[0].field[target_field][contact_field].value
        # load expected data as JSON (checks for JSON format) and assert it is same
        assert json.loads(raw_data)['contact'][contact_field] == json.loads(new_value)
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_json_generator_string_field(sdc_builder, sdc_executor):
    """Test JSON Generator processor with invalid input field. The pipeline would look like:

        dev_raw_data_source >> json_generator >> wiretap
    """
    raw_data = '{ "contact": { "name": "Jane Smith" } }'
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    json_generator = pipeline_builder.add_stage('JSON Generator', type='processor')
    json_generator.set_attributes(field_to_serialize='/contact/name', target_field='/result')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_generator >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # JSON_02 - Field '/contact/name' is a 'STRING' but must be a MAP, LIST_MAP, or LIST. Cannot serialize the field.
        assert 'JSON_02' == wiretap.error_records[0].header['errorCode']
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('raw_data', (
  '{ "a_map":{"a":"value", "b":1, "c":["x","y","z"]} }',  # map type
  '{ "a_map":["x","y","z"] }',  # list type
  '{ "a_map":[{"a":"value"},{"b":1} ,{"c":["x","y","z"]}] }'  # list_map type
))
def test_json_generator_valid_input_field(sdc_builder, sdc_executor, raw_data):
    """Test JSON Generator processor with valid input field. The pipeline would look like:

        dev_raw_data_source >> json_generator >> wiretap
    """
    raw_data = raw_data
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    json_generator = pipeline_builder.add_stage('JSON Generator', type='processor')
    json_generator.set_attributes(field_to_serialize='/a_map', target_field='/json')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_generator >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        #Assert and verify result
        assert json.loads(output_records[0].field['json'].value) == json.loads(raw_data)['a_map']
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('raw_data', (
  '{}',  # missing requested field path
  '{"a_map":null}',  # null field path
  '{ "a_map":"abc" }'  # string type
))
def test_json_generator_invalid_input_field(sdc_builder, sdc_executor, raw_data):
    """Test JSON Generator processor with list input field. The pipeline would look like:

        dev_raw_data_source >> json_generator >> wiretap
    """
    raw_data = raw_data
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    json_generator = pipeline_builder.add_stage('JSON Generator', type='processor')
    json_generator.set_attributes(field_to_serialize='/a_map', target_field='/json')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_generator >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        error_records = wiretap.error_records
        #Assert and verify result
        assert len(output_records) == 0
        assert len(error_records) == 1
        if raw_data == '{}':
            #"JSON_00 - Field '/a_map' does not exist in record 'rawData::0'. Cannot serialize the field."
            assert wiretap.error_records[0].header['errorCode'] == 'JSON_00'
        elif raw_data == '{"a_map":null}':
            #"JSON_01 - Field '/a_map' is set to NULL. Cannot serialize the field"
            assert wiretap.error_records[0].header['errorCode'] == 'JSON_01'
        elif raw_data == '{ "a_map":"abc" }':
            # JSON_02 - Field '/a_map' is a 'STRING' but must be a MAP, LIST_MAP, or LIST. Cannot serialize the field.
            assert wiretap.error_records[0].header['errorCode'] == 'JSON_02'
    finally:
        sdc_executor.remove_pipeline(pipeline)
