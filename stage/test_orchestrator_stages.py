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

import logging
import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-orchestrator-lib')

    return hook


@sdc_min_version('3.11.0')
def test_cron_scheduler_origin(sdc_builder, sdc_executor):
    """Test Cron Scheduler Origin. The pipeline would look like:

        cron_scheduler_source >> trash

    With Cron Expression "0/2 * * 1/1 * ? *", Cron Scheduler Origin should generate record with timestamp(DateTime)
    filed every two seconds.
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    cron_scheduler_source = pipeline_builder.add_stage('Cron Scheduler')
    cron_scheduler_source.cron_schedule = '0/2 * * 1/1 * ? *'
    trash = pipeline_builder.add_stage('Trash')

    cron_scheduler_source >> trash
    pipeline = pipeline_builder.build('Cron Scheduler Sample Pipeline')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Assert Cron Scheduler generated record output
    cron_scheduler_source_output = snapshot[cron_scheduler_source.instance_name].output
    timestamp_field = cron_scheduler_source_output[0].field['timestamp']
    assert timestamp_field.type == 'DATETIME'


@sdc_min_version('3.11.0')
def test_control_hub_api_processor(sdc_builder, sdc_executor):
    """Test Control Hub API Processor. The pipeline would look like:

        dev_raw_data_source >> control_hub_api_processor >> trash

    Call Control Hub API "https://cloud.streamsets.com/public-rest/v1/health" using Control Hub API Processor and
    update field output with the response
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.stop_after_first_batch = True

    control_hub_api_processor = pipeline_builder.add_stage('Control Hub API')
    control_hub_api_processor.control_hub_api_url = 'https://cloud.streamsets.com/public-rest/v1/health'
    control_hub_api_processor.output_field = "/"
    control_hub_api_processor.control_hub_user_name = "user"
    control_hub_api_processor.password = "password"

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> control_hub_api_processor >> trash

    pipeline = pipeline_builder.build('Control Hub API Processor Sample Pipeline')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    # Assert Cron Scheduler generated record output
    control_hub_api_processor_snapshot = snapshot[control_hub_api_processor.instance_name]
    assert len(control_hub_api_processor_snapshot.output) == 1
    assert len(control_hub_api_processor_snapshot.error_records) == 0
    assert control_hub_api_processor_snapshot.output[0].field['alive'].value == True


@sdc_min_version('3.11.0')
def test_control_hub_api_processor_invalid_credentials(sdc_builder, sdc_executor):
    """Test Control Hub API Processor. The pipeline would look like:

        dev_raw_data_source >> control_hub_api_processor >> trash

    With invalid Control Hub credentials, Control Hub API Processor sends the record to error records list.
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.stop_after_first_batch = True

    control_hub_api_processor = pipeline_builder.add_stage('Control Hub API')
    control_hub_api_processor.control_hub_api_url = 'https://cloud.streamsets.com/security/rest/v1/currentUser'
    control_hub_api_processor.output_field = "/output"
    control_hub_api_processor.control_hub_user_name = "invalid user"
    control_hub_api_processor.password = "invalid password"

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> control_hub_api_processor >> trash

    pipeline = pipeline_builder.build('Control Hub API Processor Sample Pipeline')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    # Assert Cron Scheduler generated record output
    control_hub_api_processor_snapshot = snapshot[control_hub_api_processor.instance_name]
    assert len(control_hub_api_processor_snapshot.output) == 0
    assert len(control_hub_api_processor_snapshot.error_records) == 1


@sdc_min_version('3.11.0')
def test_start_pipeline_processor(sdc_builder, sdc_executor):
    """Test Start Pipeline Origin/Processor. The pipeline would look like:

        start_pipeline1 >> start_pipeline2 >> pipeline_finisher

    Chain pipeline execution using start pipeline orchestrator stages. start_pipeline1 origin starts and waits
    till pipeline1 completes execution and then start pipeline2.
    """

    # Pipeline - pipeline1
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source1 = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source1.stop_after_first_batch = True
    trash1 = pipeline_builder.add_stage('Trash')
    dev_raw_data_source1 >> trash1
    pipeline1 = pipeline_builder.build('pipeline1')
    sdc_executor.add_pipeline(pipeline1)

    # Pipeline - pipeline2
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source2 = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source2.stop_after_first_batch = True
    trash2 = pipeline_builder.add_stage('Trash')
    dev_raw_data_source2 >> trash2
    pipeline2 = pipeline_builder.build('pipeline2')
    sdc_executor.add_pipeline(pipeline2)

    # Chain Pipeline Execution Sample (start_pipeline1 >> start_pipeline2 >> pipeline_finisher)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    start_pipeline1 = pipeline_builder.add_stage('Start Pipeline', type='origin')
    start_pipeline1.pipelines = [
        {
            'pipelineId': pipeline1.id
        }
    ]

    start_pipeline2 = pipeline_builder.add_stage('Start Pipeline', type='processor')
    start_pipeline2.pipelines = [
        {
            'pipelineId': pipeline2.id
        }
    ]

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    start_pipeline1 >> start_pipeline2 >> pipeline_finisher
    pipeline = pipeline_builder.build('Chain Pipeline Execution Sample')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    # Assert start_pipeline1 record output
    start_pipeline1_output = snapshot[start_pipeline1.instance_name].output
    assert len(start_pipeline1_output) == 1
    assert start_pipeline1_output[0].field['success'].value == True
    for key, value in start_pipeline1_output[0].field.items():
        if pipeline1.id in key:
            assert start_pipeline1_output[0].field[key]['success'] == True
            assert start_pipeline1_output[0].field[key]['pipelineStatus'] == "FINISHED"

    # Assert start_pipeline2 record output - start_pipeline2 output should contain output of both pipelines
    start_pipeline2_output = snapshot[start_pipeline2.instance_name].output
    assert len(start_pipeline2_output) == 1
    assert start_pipeline2_output[0].field['success'].value == True
    for key, value in start_pipeline2_output[0].field.items():
        if pipeline1.id in key or pipeline2.id in key:
            assert start_pipeline2_output[0].field[key]['success'] == True
            assert start_pipeline2_output[0].field[key]['pipelineStatus'] == "FINISHED"
