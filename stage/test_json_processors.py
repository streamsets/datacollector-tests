# Copyright 2017 StreamSets Inc.
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
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, too-many-locals


@pytest.mark.parametrize('raw_data', (
  '"A\\u0001\\r\\n\\u000C B\\r\\n C"', # string type
  '["A\\u0001\\r\\n\\u000C B\\r\\n C", "Second Item"]', # list type
  '{ "content" : "A\\u0001\\r\\n\\u000C B\\r\\n C" }' # map type
))
@pytest.mark.parametrize('target_field', ('result', 'text'))
def test_json_parser(sdc_builder, sdc_executor, raw_data, target_field):
    """Test JSON parser processor. We also test removal of ASCII control characters.
    The pipeline would look like:

        dev_raw_data_source >> json_parser >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    json_parser = pipeline_builder.add_stage('JSON Parser', type='processor')
    json_parser.set_attributes(field_to_parse='/text', ignore_control_characters=True, target_field=f'/{target_field}')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> json_parser >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # remove ASCII control characters in the expected result
    expected = json.loads(raw_data.encode('ascii', 'ignore').decode())
    assert expected == snapshot[json_parser.instance_name].output[0].field[target_field]


def test_json_parser_input_error(sdc_builder, sdc_executor):
    """Test JSON parser processor with an invalid input value. The pipeline would look like:

        dev_raw_data_source >> json_parser >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='{ "A" }')
    json_parser = pipeline_builder.add_stage('JSON Parser', type='processor')
    json_parser.set_attributes(field_to_parse='/text', target_field='result')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> json_parser >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    #JSONP_03 - Cannot parse the JSON field
    assert 'JSONP_03' == snapshot[json_parser.instance_name].error_records[0].header['errorCode']


@pytest.mark.parametrize('contact_field', ('address', 'email', 'phone'))
@pytest.mark.parametrize('target_field', ('result', 'newcontact'))
@sdc_min_version('2.7.0.0-SNAPSHOT')
def test_json_generator(sdc_builder, sdc_executor, contact_field, target_field):
    """Test JSON Generator processor. The pipeline would look like:

        dev_raw_data_source >> json_generator >> trash
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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    json_generator = pipeline_builder.add_stage('JSON Generator', type='processor')
    json_generator.set_attributes(field_to_serialize=f'/contact/{contact_field}',
                                  target_field=f'/{target_field}/{contact_field}')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> json_generator >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[json_generator.instance_name].output[0].field[target_field][contact_field].value
    # load expected data as JSON (checks for JSON format) and assert it is same
    assert json.loads(raw_data)['contact'][contact_field] == json.loads(new_value)


def test_json_generator_string_field(sdc_builder, sdc_executor):
    """Test JSON Generator processor with invalid input field. The pipeline would look like:

        dev_raw_data_source >> json_generator >> trash
    """
    raw_data = '{ "contact": { "name": "Jane Smith" } }'
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    json_generator = pipeline_builder.add_stage('JSON Generator', type='processor')
    json_generator.set_attributes(field_to_serialize='/contact/name', target_field='/result')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> json_generator >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    #JSON_02 - Field '/contact/name' is a 'STRING' but must be a MAP, LIST_MAP, or LIST. Cannot serialize the field.
    assert 'JSON_02' == snapshot[json_generator.instance_name].error_records[0].header['errorCode']
