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
import string
import uuid

from streamsets.sdk.exceptions import ValidationError
from streamsets.sdk.utils import get_random_string
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import Version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        # Add the orchestrator library, but only for version 3.11.0 and higher, on older versions the test itself
        # will be properly skipped due to sdc_min_version annotation.
        if Version(data_collector.version) >= Version("3.11.0"):
            data_collector.add_stage_lib('streamsets-datacollector-orchestrator-lib')

    return hook


@sdc_min_version('3.11.0')
def test_cron_scheduler_origin(sdc_builder, sdc_executor):
    """Test Cron Scheduler Origin. The pipeline would look like:

        cron_scheduler_source >> wiretap

    With Cron Expression "0/2 * * 1/1 * ? *", Cron Scheduler Origin should generate record with timestamp(DateTime)
    filed every two seconds.

    Also, testing multiple pipelines with Cron Scheduler origin works without any issue.
    """

    pipeline1, wiretap1 = _create_cron_pipeline(sdc_builder, 'Cron Scheduler Sample Pipeline1')
    sdc_executor.add_pipeline(pipeline1)
    sdc_executor.validate_pipeline(pipeline1)

    pipeline2, wiretap2 = _create_cron_pipeline(sdc_builder, 'Cron Scheduler Sample Pipeline2')
    sdc_executor.add_pipeline(pipeline2)
    sdc_executor.validate_pipeline(pipeline2)

    sdc_executor.start_pipeline(pipeline1)
    sdc_executor.wait_for_pipeline_metric(pipeline1, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(pipeline1)

    sdc_executor.start_pipeline(pipeline2)
    sdc_executor.wait_for_pipeline_metric(pipeline2, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(pipeline2)

    _validate_cron_scheduler_output(wiretap1)
    _validate_cron_scheduler_output(wiretap2)


def _create_cron_pipeline(sdc_builder, title):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    cron_scheduler_source = pipeline_builder.add_stage('Cron Scheduler')
    cron_scheduler_source.cron_schedule = '0/2 * * 1/1 * ? *'
    wiretap = pipeline_builder.add_wiretap()
    cron_scheduler_source >> wiretap.destination
    return pipeline_builder.build(title), wiretap


def _validate_cron_scheduler_output(wiretap):
    # Assert Cron Scheduler generated record output
    timestamp_field = wiretap.output_records[0].field['timestamp']
    assert timestamp_field.type == 'DATETIME'


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
    if Version(sdc_builder.version) >= Version('4.0.0'):
        control_hub_api_processor.authentication_type = 'USER_PASSWORD'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> control_hub_api_processor >> wiretap.destination

    pipeline = pipeline_builder.build('Control Hub API Processor Sample Pipeline')
    sdc_executor.add_pipeline(pipeline)

    if Version(sdc_builder.version) >= Version('5.6.0'):
        with pytest.raises(ValidationError) as ex:
            sdc_executor.validate_pipeline(pipeline)

        assert 'CONTROL_HUB_08' in ex.value.issues['stageIssues'][control_hub_api_processor.instance_name][0]['message']
    else:
        sdc_executor.validate_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert Cron Scheduler generated record output
        assert len(wiretap.output_records) == 0
        assert len(wiretap.error_records) == 1


@sdc_min_version('3.16.0')
def test_start_pipeline_processor(sdc_builder, sdc_executor):
    """Test Start Pipeline Origin/Processor. The pipeline would look like:

        start_pipeline1 >> start_pipeline2 >> pipeline_finisher
        start_pipeline >> wiretap

    Chain pipeline execution using start pipeline orchestrator stages. start_pipeline1 origin starts and waits
    till pipeline1 completes execution and then start pipeline2.
    """

    start_pipeline_stage_label = 'Start Pipeline'
    metrics_output_generated = False
    if Version(sdc_builder.version) >= Version('3.17.0'):
        start_pipeline_stage_label = 'Start Pipelines'
        metrics_output_generated = True

    # Pipeline - pipeline1
    pipeline1 = _create_batch_pipeline(sdc_builder, 'test_start_pipeline_processor1')
    sdc_executor.add_pipeline(pipeline1)

    # Pipeline - pipeline2
    unique_title = str(uuid.uuid4())
    pipeline2 = _create_batch_pipeline(sdc_builder, unique_title)
    sdc_executor.add_pipeline(pipeline2)

    # Chain Pipeline Execution Sample (start_pipeline1 >> start_pipeline2 >> pipeline_finisher)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    start_pipeline1 = pipeline_builder.add_stage(start_pipeline_stage_label, type='origin')
    if Version(sdc_builder.version) >= Version('3.17.0'):
        start_pipeline1.task_name = 'task1'
    else:
        start_pipeline1.unique_task_name = 'task1'
    start_pipeline1.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline1.id
        }
    ]

    start_pipeline2 = pipeline_builder.add_stage(start_pipeline_stage_label, type='processor')
    if Version(sdc_builder.version) >= Version('3.17.0'):
        start_pipeline2.task_name = 'task2'
    else:
        start_pipeline2.unique_task_name = 'task2'
    start_pipeline2.pipelines = [
        {
            'pipelineIdType': 'TITLE',
            'pipelineId': unique_title
        }
    ]

    wiretap1 = pipeline_builder.add_wiretap()
    wiretap2 = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    start_pipeline1 >> [wiretap1.destination, start_pipeline2]
    start_pipeline2 >> [wiretap2.destination, pipeline_finisher]

    pipeline = pipeline_builder.build('Chain Pipeline Execution Sample')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Assert start_pipeline1 record output
    assert len(wiretap1.output_records) == 1
    _validate_start_pipeline_output(wiretap1.output_records[0].field['orchestratorTasks'],
                                    'task1',
                                    pipeline1,
                                    True,
                                    metrics_output_generated)

    # Assert start_pipeline2 record output - start_pipeline2 output should contain output of both pipelines
    assert len(wiretap2.output_records) == 1
    _validate_start_pipeline_output(wiretap2.output_records[0].field['orchestratorTasks'],
                                    'task1',
                                    pipeline1,
                                    True,
                                    metrics_output_generated)
    _validate_start_pipeline_output(wiretap2.output_records[0].field['orchestratorTasks'],
                                    'task2',
                                    pipeline2,
                                    True,
                                    metrics_output_generated)


@sdc_min_version('3.16.0')
def test_wait_for_completion_processor(sdc_builder, sdc_executor):
    """Test Wait For Pipeline Completion Processor."""

    start_pipeline_stage_label = 'Start Pipeline'
    wait_for_completion_stage_label = 'Wait for Pipeline Completion'
    if Version(sdc_builder.version) >= Version('3.17.0'):
        start_pipeline_stage_label = 'Start Pipelines'
        wait_for_completion_stage_label = 'Wait for Pipelines'

    # Pipeline - pipeline1
    pipeline1 = _create_batch_pipeline(sdc_builder, 'test_wait_for_completion_processor')
    sdc_executor.add_pipeline(pipeline1)

    # Pipeline - pipeline2
    pipeline2 = _create_batch_pipeline(sdc_builder, 'test_wait_for_completion_processor2')
    sdc_executor.add_pipeline(pipeline2)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source1 = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source1.stop_after_first_batch = True

    start_pipeline1 = pipeline_builder.add_stage(start_pipeline_stage_label, type='processor')
    if Version(sdc_builder.version) >= Version('3.17.0'):
        start_pipeline1.task_name = 'task1'
    else:
        start_pipeline1.unique_task_name = 'task1'
    start_pipeline1.run_in_background = True
    start_pipeline1.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline1.id
        }
    ]

    start_pipeline2 = pipeline_builder.add_stage(start_pipeline_stage_label, type='processor')
    if Version(sdc_builder.version) >= Version('3.17.0'):
        start_pipeline2.task_name = 'task2'
    else:
        start_pipeline2.unique_task_name = 'task2'
    start_pipeline2.run_in_background = True
    start_pipeline2.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline2.id
        }
    ]

    wait_for_pipeline_completion = pipeline_builder.add_stage(wait_for_completion_stage_label)
    wiretap1 = pipeline_builder.add_wiretap()
    wiretap2 = pipeline_builder.add_wiretap()
    wiretap3 = pipeline_builder.add_wiretap()

    dev_raw_data_source1 >> [start_pipeline1, start_pipeline2]
    start_pipeline1 >> [wait_for_pipeline_completion, wiretap1.destination]
    start_pipeline2 >> [wait_for_pipeline_completion, wiretap2.destination]
    wait_for_pipeline_completion >> wiretap3.destination

    pipeline = pipeline_builder.build('Chain Pipeline Execution Sample2')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Assert start_pipeline1 record output
    start_pipeline1_output = wiretap1.output_records
    assert len(start_pipeline1_output) == 1
    _validate_start_pipeline_output(start_pipeline1_output[0].field['orchestratorTasks'], 'task1', pipeline1, False)

    # Assert start_pipeline2 record output
    start_pipeline2_output = wiretap2.output_records
    assert len(start_pipeline2_output) == 1
    _validate_start_pipeline_output(start_pipeline2_output[0].field['orchestratorTasks'], 'task2', pipeline2, False)

    # Assert wait_for_pipeline_completion record output
    wait_for_pipeline_completion_output = wiretap3.output_records
    assert len(wait_for_pipeline_completion_output) == 1
    _validate_start_pipeline_output(wait_for_pipeline_completion_output[0].field['orchestratorTasks'], 'task1', pipeline1, True)
    _validate_start_pipeline_output(wait_for_pipeline_completion_output[0].field['orchestratorTasks'], 'task2', pipeline2, True)


@sdc_min_version('3.20.0')
def test_stopping_pipeline_on_error(sdc_builder, sdc_executor):
    """Test for https://issues.streamsets.com/browse/SDC-16025:
    Use Case:
    If the Error field of the record output of a Start Pipeline or State Job processor is not null, user would like
    to both stop the orchestration pipeline and put it in a pipeline error state.

    Proposed Solution:
    Add a checkbox to the To Error destination called Stop Pipeline on Error (unchecked by default) that also puts
    the pipeline in a RUN_ERROR state.

    When Stop Pipeline on Error is checked, a text box field appears labeled Error Record Message where the user can
    use record EL to craft their own error record, potentially using the orchestration stage record output.
    """

    # Pipeline - pipeline1
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source1 = pipeline_builder.add_stage('Dev Raw Data Source')
    to_error = pipeline_builder.add_stage('To Error')
    to_error.stop_pipeline_on_error = True
    to_error.error_message = 'Failed to run pipeline1 due to random error'
    dev_raw_data_source1 >> to_error
    pipeline1 = pipeline_builder.build('Pipeline1')
    sdc_executor.add_pipeline(pipeline1)

    # Orchestrator pipeline - orchestrator_pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    start_pipeline1 = pipeline_builder.add_stage('Start Pipelines', type='origin')
    start_pipeline1.task_name = 't1'
    start_pipeline1.pipelines = [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline1.id
        }
    ]

    stream_selector = pipeline_builder.add_stage('Stream Selector')

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    pipeline_error = pipeline_builder.add_stage('To Error')
    pipeline_error.stop_pipeline_on_error = True
    pipeline_error.error_message = '${record:value("/orchestratorTasks/t1/pipelineResults[0]/pipelineStatusMessage")}'

    start_pipeline1 >> stream_selector >> pipeline_finisher
    stream_selector >> pipeline_error

    stream_selector.condition = [dict(outputLane=stream_selector.output_lanes[0],
                                      predicate='${record:value("/orchestratorTasks/t1/success") == true}'),
                                 dict(outputLane=stream_selector.output_lanes[1],
                                      predicate='default')]

    orchestrator_pipeline = pipeline_builder.build('Orchestrator Pipeline')
    sdc_executor.add_pipeline(orchestrator_pipeline)

    sdc_executor.validate_pipeline(orchestrator_pipeline)

    sdc_executor.start_pipeline(orchestrator_pipeline, wait=False).wait_for_status(status='RUN_ERROR')

    status = sdc_executor.get_pipeline_status(orchestrator_pipeline).response.json()
    assert status.get('status') == 'RUN_ERROR'
    assert 'Failed to run pipeline1 due to random error' in status.get('message')


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('propagate', [True, False])
def test_start_pipeline_origin_propagate_runtime_parameters(sdc_builder, sdc_executor, propagate):

    group = get_random_string(string.ascii_lowercase, 8)

    # Triggered pipeline setup - Start
    pipeline01_builder = sdc_builder.get_pipeline_builder()
    pipeline01_origin = pipeline01_builder.add_stage('Dev Raw Data Source')
    pipeline01_origin.stop_after_first_batch = True
    pipeline01_wiretap = pipeline01_builder.add_wiretap()
    pipeline01_evaluator = pipeline01_builder.add_stage('Expression Evaluator')
    pipeline01_evaluator.field_expressions = \
        [
            {
                'fieldToSet': '/f4',
                'expression': '${sdc:id()}'
            },
            {
                'fieldToSet': '/f5',
                'expression': 'Token'
            },
            {
                'fieldToSet': '/f6',
                'expression': '${Key01}'
            },
            {
                'fieldToSet': '/f7',
                'expression': '${Key02}'
            }
        ]
    pipeline01_origin >> pipeline01_evaluator
    pipeline01_evaluator >> pipeline01_wiretap.destination
    pipeline01 = pipeline01_builder.build(f'Triggered pipeline @ SDC :: {group}')
    pipeline01.add_parameters(Key01='ValueFromTriggeredPipeline01',
                                       Key02='ValueFromTriggeredPipeline02')
    sdc_executor.add_pipeline(pipeline01)
    sdc_executor.validate_pipeline(pipeline01)
    # Triggered pipeline setup - End

    # Triggering pipeline setup - Start
    pipeline02_builder = sdc_builder.get_pipeline_builder()
    pipeline02_origin = pipeline02_builder.add_stage('Start Pipelines', type='origin')
    pipeline02_origin.task_name = 'Task'
    pipeline02_origin.pipelines = \
    [
        {
            'pipelineIdType': 'ID',
            'pipelineId': pipeline01.id,
            'runtimeParameters': '''{
                                        "Key01": "ValueFromTriggeringPipeline01",
                                        "Key02": "ValueFromTriggeringPipeline02"
                                    }''',
            'propagateRuntimeParameters': propagate
        }
    ]
    pipeline02_wiretap = pipeline02_builder.add_wiretap()
    pipeline02_origin >> pipeline02_wiretap.destination
    pipeline02 = pipeline02_builder.build(f'Triggering pipeline @ SDC :: {group}')
    sdc_executor.add_pipeline(pipeline02)
    sdc_executor.validate_pipeline(pipeline02)
    # Triggering pipeline setup - End

    # Actual execution - Start
    try:
        sdc_executor.start_pipeline(pipeline02).wait_for_finished()
        pipeline01_records = pipeline01_wiretap.output_records
        assert len(pipeline01_records) == 1
        assert pipeline01_records[0].field['f4'] == sdc_executor.id
        if propagate:
            assert pipeline01_records[0].field['f6'] == 'ValueFromTriggeringPipeline01'
        else:
            assert pipeline01_records[0].field['f6'] == 'ValueFromTriggeredPipeline01'
    finally:
        if sdc_executor.get_pipeline_status(pipeline01).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline01)
        if sdc_executor.get_pipeline_status(pipeline02).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline02)
    # Actual execution - End


def _create_batch_pipeline(sdc_builder, title):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source1 = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source1.stop_after_first_batch = True
    trash1 = pipeline_builder.add_stage('Trash')
    dev_raw_data_source1 >> trash1
    return pipeline_builder.build(title)


def _validate_start_pipeline_output(orchestrator_tasks_field,
                                    task_name,
                                    pipeline,
                                    check_for_completion,
                                    check_metrics=False):
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
            if check_metrics:
                assert pipeline_results[key]['pipelineMetrics'] is not None
                assert pipeline_results[key]['pipelineMetrics']['pipeline'] is not None
                assert pipeline_results[key]['pipelineMetrics']['pipeline']['inputRecords'] == 1
                assert pipeline_results[key]['pipelineMetrics']['pipeline']['outputRecords'] == 1
                assert pipeline_results[key]['pipelineMetrics']['pipeline']['errorRecords'] == 0
                assert pipeline_results[key]['pipelineMetrics']['pipeline']['errorMessages'] == 0
                assert pipeline_results[key]['pipelineMetrics']['stages'] is not None
