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
from streamsets.testframework.sdc_models import CustomLib

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PMML_IRIS_MODEL_PATH = '/resources/resources/pmml_iris_model/iris_rf.pmml'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib(CustomLib('streamsets-datacollector-pmml-lib', '1.5.0-SNAPSHOT'))
    return hook


@sdc_min_version('3.5.0')
def test_tensorflow_evaluator(sdc_builder, sdc_executor):
    """Test PMML Evaluator processor. The pipeline would look like:

        dev_raw_data_source >> pmml_evaluator >> trash

    With given raw_data below, PMML Evaluator processor evaluates each record using the
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

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    pmml_evaluator = pipeline_builder.add_stage('PMML Evaluator')
    pmml_input_configs = [{"pmmlFieldName": "Petal.Length", "fieldName": "/petalLength"},
                          {"pmmlFieldName": "Petal.Width", "fieldName": "/petalWidth"},
                          {"pmmlFieldName": "Sepal.Length", "fieldName": "/sepalLength"},
                          {"pmmlFieldName": "Sepal.Width", "fieldName": "/sepalWidth"}]
    pmml_evaluator.set_attributes(input_configs=pmml_input_configs,
                                  model_output_fields=['Predicted_Species',
                                                       'Probability_setosa',
                                                       'Probability_versicolor',
                                                       'Probability_virginica'],
                                  output_field='/output',
                                  saved_model_file_path=PMML_IRIS_MODEL_PATH)

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> pmml_evaluator >> trash
    pipeline = pipeline_builder.build('PMML evaluator for IRIS Model')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # assert TensorFlow Model evaluation Output
    pmml_output = snapshot[pmml_evaluator.instance_name].output
    assert pmml_output[0].value['value']['output']['type'] == 'LIST_MAP'
    outputField = pmml_output[0].value['value']['output']['value']
    predicted_species_item = [item for item in outputField if item['sqpath'] == '/output/Predicted_Species'][0]
    assert predicted_species_item['type'] == 'STRING'
    assert predicted_species_item['value'] == 'virginica'
