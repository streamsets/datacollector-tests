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

import logging
import tempfile

import pytest

from collections import namedtuple
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')

    return hook


@pytest.fixture(scope='function')
def generator_trash_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      records_to_be_generated=10,
                                      delay_between_batches=1000)
    trash = builder.add_stage('Trash')

    dev_data_generator >> trash

    yield builder


@pytest.fixture(scope='function')
def generator_finisher_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      records_to_be_generated=10,
                                      delay_between_batches=1000)
    finisher = builder.add_stage('Pipeline Finisher Executor')

    dev_data_generator >> finisher

    yield builder


@pytest.fixture(scope='function')
def generator_failure_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      records_to_be_generated=10,
                                      delay_between_batches=1000)
    jython = builder.add_stage('Jython Evaluator')
    jython.script = '1 / 0'  # ~ throw exception and stop the pipeline

    trash = builder.add_stage('Trash')

    dev_data_generator >> jython >> trash

    yield builder


@pytest.fixture(scope='function')
def successful_receiver_pipeline(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Directory', type='origin')

    origin.set_attributes(batch_wait_time_in_secs=1,
                          data_format='SDC_JSON',
                          file_name_pattern='sdc*.sdc',
                          file_post_processing='DELETE',
                          files_directory=tempfile.gettempdir(),
                          process_subdirectories=True)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build('Succeeding Lifecycle Receiver')
    yield namedtuple('Pipeline', ['pipeline', 'wiretap'])(pipeline, wiretap)


@pytest.fixture(scope='function')
def failing_receiver_pipeline(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Directory', type='origin')

    origin.set_attributes(batch_wait_time_in_secs=1,
                          data_format='SDC_JSON',
                          file_name_pattern='sdc*.sdc',
                          file_post_processing='DELETE',
                          files_directory=tempfile.gettempdir())

    jython = builder.add_stage('Jython Evaluator')
    jython.script = '1 / 0'  # ~ throw exception and stop the pipeline
    trash = builder.add_stage('Trash')

    origin >> jython >> trash

    pipeline = builder.build('Failing Lifecycle Receiver')
    pipeline.configuration['shouldRetry'] = False

    yield pipeline


@sdc_min_version('6.0.0')
def test_start_event(generator_trash_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event on pipeline start."""
    start_stage = generator_trash_builder.add_start_event_stage('Write to File')
    start_stage.directory = tempfile.gettempdir()

    start_event_pipeline = generator_trash_builder.build('Start Event')

    sdc_executor.add_pipeline(start_event_pipeline, successful_receiver_pipeline.pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * Wiretap output records has one record
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start

        sdc_executor.start_pipeline(successful_receiver_pipeline.pipeline)
        sdc_executor.start_pipeline(start_event_pipeline).wait_for_finished()

        # And validate that the event arrived to the receiver pipeline
        sdc_executor.wait_for_pipeline_metric(successful_receiver_pipeline.pipeline, 'input_record_count', 1)
        assert len(successful_receiver_pipeline.wiretap.output_records) == 1
        record = successful_receiver_pipeline.wiretap.output_records[0]
        assert record is not None
        assert record.header['values']['sdc.event.type'] == 'pipeline-start'
        assert record.field['user'].value == 'admin'

    finally:
        stop_pipelines(sdc_executor, [start_event_pipeline, successful_receiver_pipeline.pipeline])


@sdc_min_version('6.0.0')
def test_start_event_with_job_info(generator_trash_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate jobId and jobName to pipeline start event"""
    start_stage = generator_trash_builder.add_start_event_stage('Write to File')
    start_stage.directory = tempfile.gettempdir()

    start_event_pipeline = generator_trash_builder.build('Start Event')
    start_event_pipeline.add_parameters(JOB_ID='stfJobId', JOB_NAME='stfJobName')
    sdc_executor.add_pipeline(start_event_pipeline, successful_receiver_pipeline.pipeline)

    try:
        sdc_executor.start_pipeline(successful_receiver_pipeline.pipeline, wait=False)
        sdc_executor.start_pipeline(start_event_pipeline).wait_for_finished()

        # And validate that the event arrived to the receiver pipeline
        sdc_executor.wait_for_pipeline_metric(successful_receiver_pipeline.pipeline, 'input_record_count', 1)
        assert len(successful_receiver_pipeline.wiretap.output_records) == 1
        record = successful_receiver_pipeline.wiretap.output_records[0]
        assert record is not None
        assert record.header['values']['sdc.event.type'] == 'pipeline-start'
        assert record.field['user'].value == 'admin'
        assert record.field['jobId'].value == 'stfJobId'
        assert record.field['jobName'].value == 'stfJobName'
    finally:
        stop_pipelines(sdc_executor, [start_event_pipeline, successful_receiver_pipeline.pipeline])


@sdc_min_version('6.0.0')
def test_stop_event_user_action(generator_trash_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event when pipeline is stopped by user."""
    stop_stage = generator_trash_builder.add_stop_event_stage('Write to File')
    stop_stage.directory = tempfile.gettempdir()

    stop_event_pipeline = generator_trash_builder.build('Stop Event - User Action')

    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline.pipeline)

    # Since there will be exactly one event generated we need to make sure that:
    # * Wiretap output records has one record
    # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
    sdc_executor.start_pipeline(successful_receiver_pipeline.pipeline, wait=False)
    sdc_executor.start_pipeline(stop_event_pipeline)
    sdc_executor.stop_pipeline(stop_event_pipeline)

    # And validate that the event arrived to the receiver pipeline
    sdc_executor.wait_for_pipeline_metric(successful_receiver_pipeline.pipeline, 'input_record_count', 1)
    assert len(successful_receiver_pipeline.wiretap.output_records) == 1
    record = successful_receiver_pipeline.wiretap.output_records[0]

    assert record is not None
    assert record.header['values']['sdc.event.type'] == 'pipeline-stop'
    assert record.field['reason'].value == 'USER_ACTION'

    stop_pipelines(sdc_executor, [stop_event_pipeline, successful_receiver_pipeline.pipeline])


@sdc_min_version('6.0.0')
def test_stop_event_with_job_info(generator_trash_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate jobId and jobName to pipeline stop event"""
    stop_stage = generator_trash_builder.add_stop_event_stage('Write to File')
    stop_stage.directory = tempfile.gettempdir()

    stop_event_pipeline = generator_trash_builder.build('Stop Event - Job Info')
    stop_event_pipeline.add_parameters(JOB_ID='stfJobId', JOB_NAME='stfJobName')
    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline.pipeline)

    try:
        sdc_executor.start_pipeline(successful_receiver_pipeline.pipeline, wait=False)
        sdc_executor.start_pipeline(stop_event_pipeline)
        sdc_executor.stop_pipeline(stop_event_pipeline)

        # Validate that the event arrived to the receiver pipeline
        sdc_executor.wait_for_pipeline_metric(successful_receiver_pipeline.pipeline, 'input_record_count', 1, timeout_sec=120)
        assert len(successful_receiver_pipeline.wiretap.output_records) == 1
        record = successful_receiver_pipeline.wiretap.output_records[0]

        assert record is not None
        assert record.header['values']['sdc.event.type'] == 'pipeline-stop'
        assert record.field['reason'].value == 'USER_ACTION'
        assert record.field['jobId'].value == 'stfJobId'
        assert record.field['jobName'].value == 'stfJobName'
    finally:
        stop_pipelines(sdc_executor, [stop_event_pipeline, successful_receiver_pipeline.pipeline])


@sdc_min_version('6.0.0')
def test_stop_event_finished(generator_finisher_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event when pipeline finishes."""
    stop_stage = generator_finisher_builder.add_stop_event_stage('Write to File')
    stop_stage.directory = tempfile.gettempdir()

    stop_event_pipeline = generator_finisher_builder.build('Stop Event - Finished')

    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline.pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * Wiretap output records has one record
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
        sdc_executor.start_pipeline(successful_receiver_pipeline.pipeline, wait=False)
        sdc_executor.start_pipeline(stop_event_pipeline).wait_for_finished()

        # Validate that the event arrived to the receiver pipeline
        sdc_executor.wait_for_pipeline_metric(successful_receiver_pipeline.pipeline, 'input_record_count', 1)
        assert len(successful_receiver_pipeline.wiretap.output_records) == 1
        record = successful_receiver_pipeline.wiretap.output_records[0]

        assert record is not None
        assert record.header['values']['sdc.event.type'] == 'pipeline-stop'
        assert record.field['reason'].value == 'FINISHED'

    finally:
        stop_pipelines(sdc_executor, [stop_event_pipeline, successful_receiver_pipeline.pipeline])


@sdc_min_version('6.0.0')
def test_stop_event_failure(generator_failure_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event when pipeline crashes."""
    stop_stage = generator_failure_builder.add_stop_event_stage('Write to File')
    stop_stage.directory = tempfile.gettempdir()

    stop_event_pipeline = generator_failure_builder.build('Stop Event - Failure')
    stop_event_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline.pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * Wiretap output records has one record
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
        sdc_executor.start_pipeline(successful_receiver_pipeline.pipeline, wait=False)
        with pytest.raises(Exception) as error:
            sdc_executor.start_pipeline(stop_event_pipeline)
        assert "SCRIPTING_06" in error.value.message, f'Expected a SCRIPTING_06 error, got "{error.value.message}" instead'

        # Validate that the event arrived to the receiver pipeline
        sdc_executor.wait_for_pipeline_metric(successful_receiver_pipeline.pipeline, 'input_record_count', 1)
        assert len(successful_receiver_pipeline.wiretap.output_records) == 1
        record = successful_receiver_pipeline.wiretap.output_records[0]

        assert record is not None
        assert record.header['values']['sdc.event.type'] == 'pipeline-stop'
        assert record.field['reason'].value == 'FAILURE'

    finally:
        stop_pipelines(sdc_executor, [stop_event_pipeline, successful_receiver_pipeline.pipeline])


def stop_pipelines(sdc_executor, pipelines):
    for pipeline in pipelines:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)