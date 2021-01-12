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

DATABRICKS_ML_STRING_MODEL_PATH = '/resources/resources/databricks_ml_model/20news_pipeline'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-databricks-ml_2-lib')

    return hook


@sdc_min_version('3.5.0')
def test_databricks_ml_evaluator_string_model(sdc_builder, sdc_executor):
    """Test Databricks ML Evaluator processor. The pipeline would look like:

        dev_raw_data_source >> databricks_ml_evaluator >> trash

    With given raw_data below, Databricks ML Evaluator processor evaluates each record using the
    sample Databricks ML exported Model.
    """
    raw_data = """
        {
          "topic": "sci.space",
          "text": "NASA sent a rocket to outer space with scientific ice cream."
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=raw_data,
                                                                                           stop_after_first_batch=True)

    databricks_ml_evaluator = pipeline_builder.add_stage('Databricks ML Evaluator')

    databricks_ml_evaluator.set_attributes(input_root_field='/',
                                           model_output_columns=['label', 'prediction', 'probability'],
                                           output_field='/output',
                                           saved_model_path=DATABRICKS_ML_STRING_MODEL_PATH)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> databricks_ml_evaluator >> wiretap.destination
    pipeline = pipeline_builder.build('Databricks ML String Input')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # assert Databricks ML Model evaluation Output
    ml_output = wiretap.output_records
    output_field = ml_output[0].field['output']
    assert isinstance(output_field, dict)
    assert isinstance(output_field['prediction'].value, float)
    assert output_field['prediction'].value == 7.0
    assert isinstance(output_field['label'].value, float)
    assert output_field['label'].value == 7.0
    assert isinstance(output_field['probability'], dict)
    assert isinstance(output_field['probability']['type'].value, int)
