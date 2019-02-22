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

import pytest


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')

    return hook


def test_groovy_evaluator(sdc_builder, sdc_executor):
    """Test failing stage and ensure that the log will correctly contain the exception state information like
       stage name, thread id and runner.

        dev_raw_data_source >> groovy_evaluator >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{}')

    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor')
    groovy_evaluator.set_attributes(script='1/0')

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> groovy_evaluator >> trash
    pipeline = pipeline_builder.build('Failing Groovy Pipeline')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(status='RUN_ERROR', ignore_errors=True)

    # Get logs from PipeRunner class
    # TODO: TLKT-166: Make Log class expose underlying log lines
    # TODO: TLKT-168: DataCollector.get_logs accepts pipeline arg but does filter logs based off of that
    log_line = next((line for line in reversed(sdc_executor.get_logs()._data) if line.get('category') == 'PipeRunner' and 'Division by zero' in line.get('message')), None)

    assert log_line is not None
    assert log_line['s-runner'] == '0'
    assert log_line['thread']
    assert 'GroovyEvaluator' in log_line['s-stage']    
