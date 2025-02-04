# Copyright 2025 StreamSets Inc.
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
import logging

from streamsets.sdk.exceptions import ValidationError

logger = logging.getLogger(__name__)

def test_field_expressions_issues(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Data Generator')

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(field_expressions=[
        {'fieldToSet': 'wrongField1'},
        {'fieldToSet': '/correctField'},
        {'fieldToSet': 'wrongField2'}
    ])

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> expression_evaluator >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(ValidationError) as e:
        sdc_executor.start_pipeline(pipeline)

    assert e.value.issues['issueCount'] == 2

    assert 'VALIDATION_0033' in e.value.issues['stageIssues']['ExpressionEvaluator_01'][0]['message']
    assert 'wrongField1' in e.value.issues['stageIssues']['ExpressionEvaluator_01'][0]['message']
    assert 'expressionProcessorConfigs#0:fieldToSet' == e.value.issues['stageIssues']['ExpressionEvaluator_01'][0]['configName']

    assert 'VALIDATION_0033' in e.value.issues['stageIssues']['ExpressionEvaluator_01'][1]['message']
    assert 'wrongField2' in e.value.issues['stageIssues']['ExpressionEvaluator_01'][1]['message']
    assert 'expressionProcessorConfigs#2:fieldToSet' == e.value.issues['stageIssues']['ExpressionEvaluator_01'][1]['configName']

