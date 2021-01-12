# Copyright 2018 StreamSets Inc.
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


import logging

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TENSOR_FLOW_IRIS_MODEL_PATH = '/resources/resources/tensorflow_iris_model'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-tensorflow-lib')

    return hook


@sdc_min_version('3.5.0')
def test_tensorflow_evaluator(sdc_builder, sdc_executor):
    """Test TensorFlow Evaluator processor. The pipeline would look like:

        dev_raw_data_source >> field_type_converter >> tensorflow_evaluator >> trash

    With given raw_data below, TensorFlow Evaluator processor evaluate each record using the
    sample Iris classification model.
    """
    raw_data = """
        {
          "petalLength": 6.4,
          "petalWidth": 2.8,
          "sepalLength": 5.6,
          "sepalWidth": 2.2
        }
        {
          "petalLength": 5.0,
          "petalWidth": 2.3,
          "sepalLength": 3.3,
          "sepalWidth": 1.0
        }
        {
          "petalLength": 4.9,
          "petalWidth": 2.5,
          "sepalLength": 4.5,
          "sepalWidth": 1.7
        }
        {
          "petalLength": 4.9,
          "petalWidth": 3.1,
          "sepalLength": 1.5,
          "sepalWidth": 0.1
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=raw_data,
                                                                                           stop_after_first_batch=True)

    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_configs = [
        {
            'fields': ['/*'],
            'targetType': 'FLOAT',
            'dataLocale': 'en,US'
        }
    ]
    field_type_converter_fields.set_attributes(
        conversion_method='BY_FIELD',
        field_type_converter_configs=field_type_converter_configs
    )

    tensorflow_evaluator = pipeline_builder.add_stage('TensorFlow Evaluator')
    tensorflow_input_configs = [
        {
            "fields": [
                "/petalLength"
            ],
            "shape": [
                "1"
            ],
            "index": 0,
            "operation": "PetalLength",
            "tensorDataType": "FLOAT"
        },
        {
            "fields": [
                "/petalWidth"
            ],
            "shape": [
                "1"
            ],
            "index": 0,
            "operation": "PetalWidth",
            "tensorDataType": "FLOAT"
        },
        {
            "fields": [
                "/sepalLength"
            ],
            "shape": [
                "1"
            ],
            "index": 0,
            "operation": "SepalLength",
            "tensorDataType": "FLOAT"
        },
        {
            "fields": [
                "/sepalWidth"
            ],
            "shape": [
                "1"
            ],
            "index": 0,
            "operation": "SepalWidth",
            "tensorDataType": "FLOAT"
        }
    ]
    tensorflow_output_configs = [
        {
            "index": 0,
            "operation": "dnn/head/predictions/ExpandDims",
            "tensorDataType": "FLOAT"
        },
        {
            "index": 0,
            "operation": "dnn/head/predictions/probabilities",
            "tensorDataType": "FLOAT"
        }
    ]
    tensorflow_evaluator.set_attributes(
        saved_model_path=TENSOR_FLOW_IRIS_MODEL_PATH,
        model_tags=["serve"],
        input_configs=tensorflow_input_configs,
        output_configs=tensorflow_output_configs
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter_fields >> tensorflow_evaluator >> wiretap.destination
    pipeline = pipeline_builder.build('TensorFlow IRIS JSON Example Pipeline')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 4

    output_field = wiretap.output_records[0].field['output']
    assert isinstance(output_field, dict)
    assert isinstance(output_field['dnn/head/predictions/ExpandDims_0'], list)
    assert isinstance(output_field['dnn/head/predictions/ExpandDims_0'][0].value, int)
    assert output_field['dnn/head/predictions/ExpandDims_0'][0].value == 2
    assert isinstance(output_field['dnn/head/predictions/probabilities_0'], list)
    assert all([isinstance(prob.value, float) for prob in output_field['dnn/head/predictions/probabilities_0']])
