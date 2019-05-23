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
from collections import OrderedDict

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MLEAP_AIRBNB_MODEL_PATH = '/resources/resources/mleap_airbnb_model/airbnb.model.rf'

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-mleap-lib')
    return hook

@sdc_min_version('3.5.0')
def test_mleap_evaluator(sdc_builder, sdc_executor):
    """Test MLeap Evaluator processor. The pipeline would look like:

        dev_raw_data_source >> mleap_evaluator >> trash

    With given raw_data below, MLeap Evaluator processor evaluate each record using the
    sample MLeap Airbnb Price Prediction Model.
    """
    name_separator = '.'
    raw_data = """
        {
            "security_deposit": 50.0,
            "bedrooms": 3.0,
            "instant_bookable": "1.0",
            "room_type": "Entire home/apt",
            "state": "NY",
            "cancellation_policy": "strict",
            "square_feet": 1250.0,
            "number_of_reviews": 56.0,
            "extra_people": 2.0,
            "bathrooms": 2.0,
            "host_is_superhost": "1.0",
            "review_scores_rating": 90.0,
            "cleaning_fee": 30.0
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    mleap_evaluator = pipeline_builder.add_stage('MLeap Evaluator')
    mleap_input_configs = [
        {
            "pmmlFieldName": "security_deposit",
            "fieldName": "/security_deposit"
        },
        {
            "pmmlFieldName": "bedrooms",
            "fieldName": "/bedrooms"
        },
        {
            "pmmlFieldName": "instant_bookable",
            "fieldName": "/instant_bookable"
        },
        {
            "pmmlFieldName": "room_type",
            "fieldName": "/room_type"
        },
        {
            "pmmlFieldName": "state",
            "fieldName": "/state"
        },
        {
            "pmmlFieldName": "cancellation_policy",
            "fieldName": "/cancellation_policy"
        },
        {
            "pmmlFieldName": "square_feet",
            "fieldName": "/square_feet"
        },
        {
            "pmmlFieldName": "number_of_reviews",
            "fieldName": "/number_of_reviews"
        },
        {
            "pmmlFieldName": "extra_people",
            "fieldName": "/extra_people"
        },
        {
            "pmmlFieldName": "bathrooms",
            "fieldName": "/bathrooms"
        },
        {
            "pmmlFieldName": "host_is_superhost",
            "fieldName": "/host_is_superhost"
        },
        {
            "pmmlFieldName": "review_scores_rating",
            "fieldName": "/review_scores_rating"
        },
        {
            "pmmlFieldName": "cleaning_fee",
            "fieldName": "/cleaning_fee"
        }
    ]
    mleap_evaluator.set_attributes(
        saved_model_file_path=MLEAP_AIRBNB_MODEL_PATH,
        input_configs=mleap_input_configs,
        model_output_fields=['price_prediction'],
        output_field='/output'
    )

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> mleap_evaluator >> trash
    pipeline = pipeline_builder.build('MLeap Airbnb Price Prediction Model Pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # assert MLeap Model evaluation Output
    tensorflow_output = snapshot[mleap_evaluator.instance_name].output
    assert isinstance(tensorflow_output[0].field['output'], OrderedDict)
    output_field_value = tensorflow_output[0].field['output']['price_prediction'].value
    assert output_field_value == 218.2767196535019
