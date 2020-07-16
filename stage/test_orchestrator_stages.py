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
import uuid

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

    Also, testing multiple pipelines with Cron Scheduler origin works without any issue.
    """

    pipeline1 = _create_cron_pipeline(sdc_builder, 'Cron Scheduler Sample Pipeline1')
    sdc_executor.add_pipeline(pipeline1)
    sdc_executor.validate_pipeline(pipeline1)

    pipeline2 = _create_cron_pipeline(sdc_builder, 'Cron Scheduler Sample Pipeline2')
    sdc_executor.add_pipeline(pipeline2)
    sdc_executor.validate_pipeline(pipeline2)

    snapshot1 = sdc_executor.capture_snapshot(pipeline1, start_pipeline=True).snapshot
    snapshot2 = sdc_executor.capture_snapshot(pipeline2, start_pipeline=True).snapshot

    sdc_executor.stop_pipeline(pipeline1)
    sdc_executor.stop_pipeline(pipeline2)

    _validate_cron_scheduler_output(snapshot1, pipeline1.origin_stage.instance_name)
    _validate_cron_scheduler_output(snapshot2, pipeline2.origin_stage.instance_name)


def _create_cron_pipeline(sdc_builder, title):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    cron_scheduler_source = pipeline_builder.add_stage('Cron Scheduler')
    cron_scheduler_source.cron_schedule = '0/2 * * 1/1 * ? *'
    trash = pipeline_builder.add_stage('Trash')
    cron_scheduler_source >> trash
    return pipeline_builder.build(title)


def _validate_cron_scheduler_output(snapshot, instance_name):
    # Assert Cron Scheduler generated record output
    cron_scheduler_source_output = snapshot[instance_name].output
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


@sdc_min_version('3.16.0')
def test_start_pipeline_processor(sdc_builder, sdc_executor):
    """Test Start Pipeline Origin/Processor. The pipeline would look like:

        start_pipeline1 >> start_pipeline2 >> pipeline_finisher

    Chain pipeline execution using start pipeline orchestrator stages. start_pipeline1 origin starts and waits
    till pipeline1 completes execution and then start pipeline2.
    """

    # Pipeline - pipeline1
    pipeline1 = _create_batch_pipeline(sdc_builder, 'test_start_pipeline_processor1')
    sdc_executor.add_pipeline(pipeline1)

    # Pipeline - pipeline2
    unique_title = str(uuid.uuid4())
    pipeline2 = _create_batch_pipeline(sdc_builder, unique_title)
    sdc_executor.add_pipeline(pipeline2)

    # Chain Pipeline Execution Sample (start_pipeline1 >> start_pipeline2 >> pipeline_finisher)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    start_pipeline1 = pipeline_builder.add_stage('Start Pipeline', type='origin')
    start_pipeline1.unique_task_name = 'task1'
    start_pipeline1.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline1.id
        }
    ]

    start_pipeline2 = pipeline_builder.add_stage('Start Pipeline', type='processor')
    start_pipeline2.unique_task_name = 'task2'
    start_pipeline2.pipelines = [
        {
            'pipelineIdType': 'TITLE',
            'pipelineId': unique_title
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
    _validate_start_pipeline_output(start_pipeline1_output[0].field['orchestratorTasks'], 'task1', pipeline1, True)

    # Assert start_pipeline2 record output - start_pipeline2 output should contain output of both pipelines
    start_pipeline2_output = snapshot[start_pipeline2.instance_name].output
    assert len(start_pipeline2_output) == 1
    _validate_start_pipeline_output(start_pipeline2_output[0].field['orchestratorTasks'], 'task1', pipeline1, True)
    _validate_start_pipeline_output(start_pipeline2_output[0].field['orchestratorTasks'], 'task2', pipeline2, True)


@sdc_min_version('3.16.0')
def test_wait_for_completion_processor(sdc_builder, sdc_executor):
    """Test Wait For Pipeline Completion Processor."""

    # Pipeline - pipeline1
    pipeline1 = _create_batch_pipeline(sdc_builder, 'test_wait_for_completion_processor')
    sdc_executor.add_pipeline(pipeline1)

    # Pipeline - pipeline2
    pipeline2 = _create_batch_pipeline(sdc_builder, 'test_wait_for_completion_processor2')
    sdc_executor.add_pipeline(pipeline2)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source1 = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source1.stop_after_first_batch = True

    start_pipeline1 = pipeline_builder.add_stage('Start Pipeline', type='processor')
    start_pipeline1.unique_task_name = 'task1'
    start_pipeline1.run_in_background = True
    start_pipeline1.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline1.id
        }
    ]

    start_pipeline2 = pipeline_builder.add_stage('Start Pipeline', type='processor')
    start_pipeline2.unique_task_name = 'task2'
    start_pipeline2.run_in_background = True
    start_pipeline2.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline2.id
        }
    ]

    wait_for_pipeline_completion = pipeline_builder.add_stage('Wait for Pipeline Completion')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source1 >> [start_pipeline1, start_pipeline2]
    start_pipeline1 >> wait_for_pipeline_completion
    start_pipeline2 >> wait_for_pipeline_completion
    wait_for_pipeline_completion >> trash

    pipeline = pipeline_builder.build('Chain Pipeline Execution Sample2')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    # Assert start_pipeline1 record output
    start_pipeline1_output = snapshot[start_pipeline1.instance_name].output
    assert len(start_pipeline1_output) == 1
    _validate_start_pipeline_output(start_pipeline1_output[0].field['orchestratorTasks'], 'task1', pipeline1, False)

    # Assert start_pipeline2 record output
    start_pipeline2_output = snapshot[start_pipeline2.instance_name].output
    assert len(start_pipeline2_output) == 1
    _validate_start_pipeline_output(start_pipeline2_output[0].field['orchestratorTasks'], 'task2', pipeline2, False)

    # Assert wait_for_pipeline_completion record output
    wait_for_pipeline_completion_output = snapshot[wait_for_pipeline_completion.instance_name].output
    assert len(wait_for_pipeline_completion_output) == 1
    _validate_start_pipeline_output(wait_for_pipeline_completion_output[0].field['orchestratorTasks'], 'task2', pipeline2, True)
    _validate_start_pipeline_output(wait_for_pipeline_completion_output[0].field['orchestratorTasks'], 'task2', pipeline2, True)


def _create_batch_pipeline(sdc_builder, title):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source1 = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source1.stop_after_first_batch = True
    trash1 = pipeline_builder.add_stage('Trash')
    dev_raw_data_source1 >> trash1
    return pipeline_builder.build(title)


def _validate_start_pipeline_output(orchestrator_tasks_field, task_name, pipeline, check_for_completion):
    assert orchestrator_tasks_field is not None
    assert orchestrator_tasks_field[task_name] is not None

    if check_for_completion:
        assert orchestrator_tasks_field[task_name]['success'] == True

    assert orchestrator_tasks_field[task_name]['pipelineIds'] is not None
    assert len(orchestrator_tasks_field[task_name]['pipelineIds']) == 1
    assert orchestrator_tasks_field[task_name]['pipelineIds'][0] == pipeline.id
    assert orchestrator_tasks_field[task_name]['pipelineResults'] is not None
    assert len(orchestrator_tasks_field[task_name]['pipelineResults']) == 1

    pipeline_results = orchestrator_tasks_field[task_name]['pipelineResults']
    for key, value in pipeline_results.items():
        if pipeline.id in key:
            assert pipeline_results[key] is not None
            assert pipeline_results[key]['pipelineId'] == pipeline.id
            assert pipeline_results[key]['pipelineTitle'] == pipeline.title
            assert pipeline_results[key]['startedSuccessfully'] == True
            if check_for_completion:
                assert pipeline_results[key]['finishedSuccessfully'] == True
                assert pipeline_results[key]['pipelineStatus'] == 'FINISHED'
                assert pipeline_results[key]['pipelineStatusMessage'] is not None

